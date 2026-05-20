# Design Notes

这份文档记录当前代码中已经收敛的两个主题：

- 跨 shard 事务支持
- 基于 Raft lease/read-index 的线性一致性读

它不是实现 TODO 列表，而是帮助对照源码理解：哪些语义已经实现，状态推进放在哪一层，以及哪些细节刻意没有做。

## 代码地图

事务相关代码主要分布在：

- `src/coordinator/common.go`：coordinator 侧事务状态、命令和测试事件定义。
- `src/coordinator/server.go`：coordinator 状态机、2PC 推进、恢复 driver、snapshot。
- `src/coordinator/client.go`：事务客户端入口。
- `src/shardkv/common.go`：participant 侧事务 RPC、状态和命令定义。
- `src/shardkv/server.go`：participant 侧 `Prepare/Commit/Abort`，事务锁表，reconfiguration 守卫，snapshot。

线性一致性读相关代码主要分布在：

- `src/raft/raft.go`：leader lease、heartbeat majority ack、`LeaseReadIndex()`。
- `src/shardkv/server.go`：`Get` 的 read-index 路径、follower read、batching、apply 等待、`executeCh`。
- `src/shardkv/client.go`：client 侧 server 缓存与 `GetV1` 对比入口。
- `src/shardkv/test_test.go`：leader/follower lease-read 性能对比与 leader election 后恢复测试。

## 分层边界

当前实现里，最重要的边界是：协议层负责证明“某个前缀可以安全读取”，业务层负责证明“这个前缀对应的业务状态已经在本地可见”。

事务支持里：

- Coordinator 负责事务全局状态和 2PC 决议推进。
- ShardKV participant 负责本 group 内的 prepare/commit/abort 状态、锁表和实际 KV 变更。
- Reconfiguration 不由 coordinator 直接控制，而是在 ShardKV participant apply config change 时检查本地未决事务是否命中本次迁移 shard。

线性一致性读里：

- Raft 只负责 `LeaseReadIndex()`：返回一个安全的 replicated log 前缀位置。
- ShardKV 负责等待自己的 `lastAppliedIndex` 追上该位置，并在状态机线程里读取业务状态。
- Follower read 不是让 follower 自己判断安全，而是让 follower 从 leader 获取 read index，然后在本地等 apply 并读取。

## 事务支持

### 目标语义

当前事务接口由独立的 coordinator 层发起，最终在一个或多个 shardkv replica group 上执行。事务包含若干 `TxnOperation`，返回值顺序与输入操作顺序一致：

- `Get` 返回读到的值。
- `Put` 返回写入值。
- `Append` 返回追加后的最终值。
- 条件操作用于检查当前状态，返回当前值。
- abort 时客户端返回 `nil` 或 `ErrTxnAborted`，取决于调用层封装。

事务 ID 是幂等边界。同一个 `txnID` 的重复请求应返回第一次事务的最终结果，而不是重新解释新的操作序列。

### Coordinator 状态机

Coordinator 使用独立 Raft group 复制事务元数据，核心状态在 `Coordinator.stateTable` 中。状态流转定义在 `src/coordinator/common.go`：

- `TxnStatusPrepare`
- `TxnStatusCommit`
- `TxnStatusAbort`
- `TxnStatusCommitted`
- `TxnStatusAborted`

主要流程在 `Coordinator.Transaction()`：

1. 只有 coordinator leader 接受事务入口。
2. 使用 `inflightTxns` 避免同一个 `txnID` 的并发入口重复推进。
3. 对新事务，根据当前 shard config 计算 participant group 集合。
4. 将 participant 分组、原始操作下标、操作总数、primary group 和配置号一起持久化为 `TxnStatusPrepare`。
5. 状态机 apply 后启动 `executePrepare()`。
6. prepare 全部成功则持久化 `TxnStatusCommit`，否则持久化 `TxnStatusAbort`。
7. `TxnStatusCommit/Abort` apply 后，由 `executeFinalAction()` 持续向 participant 传播最终决议。
8. 最终进入 `TxnStatusCommitted/Aborted`，并唤醒等待中的客户端。

`GroupOpIndexes` 用来把各 participant 返回的 `Values` 拼回客户端原始操作顺序。最终 `Values` 会随 `TxnStatusCommitted` 一起持久化，保证重复查询不依赖某次临时 RPC 收集结果。

### Primary Group 与恢复

事务会选一个 `PrimaryGID`。在 commit 路径中，coordinator 会 best-effort 先向 primary participant 发送 commit，这样在 coordinator 故障后，其他 participant 可以通过 primary 查询最终方向。

Coordinator 还有 `recoveryDriver()`：

- leader 周期性扫描 `Prepare/Commit/Abort` 中间态事务。
- 对 `Prepare` 重新启动 `executePrepare()`。
- 对 `Commit/Abort` 重新启动最终决议传播。
- 通过 `tryEnterExecutor()` 避免同一事务同一阶段重复启动多个 executor。

这套恢复 driver 的目标是恢复协议推进，不是恢复崩溃前的 goroutine waiter。`ResultCh` / `ExecutedCh` 都是进程内通知器，不是持久化语义的一部分。

测试入口主要在 `src/coordinator/test_test.go`：

- 基础事务语义：空事务、单 participant、单 key、多操作顺序、同 `txnID` 幂等。
- 冲突与 abort：同 key 冲突、triangle conflict、abort tombstone、commit/abort 幂等。
- reconfiguration 与恢复：prepared txn 阻挡相关 shard 的 config apply、participant crash/restart、coordinator crash 后 primary resolution。
- 随机扰动：unreliable、crash、fatal crash、reconfig 与 Porcupine 事务接口线性化检查。

### Participant 状态机

ShardKV participant 侧状态定义在 `src/shardkv/common.go`：

- `TxnStatusPrepared`
- `TxnStatusCommitted`
- `TxnStatusAborted`

核心状态在 `ShardKV` 中：

- `txnStateMap`：事务状态表。
- `txnReadSet`：key 到读锁 holder 集合。
- `txnWriteSet`：key 到写锁 holder 集合。

`Prepare` 进入 shardkv 自己的 Raft 日志后才真正生效。`applyTxnPrepare()` 的检查顺序大致是：

1. 幂等检查：已存在状态则直接按现有状态返回。
2. config num 检查：participant 只接受事务绑定的配置版本。
3. shard ownership / shard readiness 检查。
4. 读写锁冲突检查。
5. 条件操作检查。
6. 成功后写入 `TxnStatusPrepared` 并持有读写锁。

`Commit` 在 apply 时执行事务操作并写入最终 `Values`，然后释放锁并进入 `TxnStatusCommitted`。`Abort` 会释放锁并写入 `TxnStatusAborted` tombstone，因此后续 late `Prepare` 可以被稳定拒绝。

### 普通 KV 与事务锁表的边界

当前实现没有把普通 `Get/Put/Append` 纳入事务锁表。事务锁只约束事务之间的冲突，不试图把整个 shardkv 变成统一事务化访问模型。

这个取舍是刻意的：

- 可以避免大幅侵入已有普通 KV 路径。
- prepared 事务的写入不会提前污染 `state`。
- 普通 KV 请求不会因为事务锁而被全面重写。

代价是：事务语义主要面向事务接口内部，不是对所有普通 KV 请求提供完整隔离层。

### Reconfiguration 交互

Prepared txn 和 shard migration 的交互采用简化但明确的规则：如果某个未决 txn 的读写集合命中本次 config change 要迁出的 shard，则延后这次 config apply。

对应逻辑在 `ShardKV.isConfigChangeValid()`：

- 先按原有规则检查配置号单调推进和待接收 shard 状态。
- 再计算本次 `shardsToSend`。
- 如果未决事务锁表中的 key 属于这些 shard，则暂不 apply 该 config。

这个设计不是阻止 shardctrler 发布配置，而是延后当前 group 应用相关配置。无关 shard 的事务不应阻塞无关 shard 的配置推进。

### Snapshot 与本地通知器

Coordinator snapshot 持久化 `stateTable`。ShardKV snapshot 持久化：

- 普通 KV state / dedup / config / shard migration 状态。
- `txnStateMap`。
- `txnReadSet`。
- `txnWriteSet`。

`ResultCh`、`ExecutedCh`、RPC waiter 等 channel 不会跨 snapshot 或 crash 恢复。它们只表示“当前进程生命周期内观察到某条命令 apply”的便利机制。

共享 helper `PersistCommand()` 的语义是：

- 反复 `rf.Start(cmd)`。
- 如果不再是 leader，则调用 `notLeaderCallback` 并返回失败。
- 如果命令 apply 并从 result channel 收到结果，则返回成功。

它不表示协议永久成功，只表示当前 leader 进程观察到了这次状态机 apply。

### 当前没有继续做的细节

- 没有把普通 KV 请求与事务请求统一到同一套锁表。
- participant 不主动联系 coordinator 查询最终决议；当前依赖 coordinator recovery driver 继续推进，participant 只在需要时查询 primary。
- reconfiguration 采用“相关 shard 延后 apply”的简化模型，不试图在所有配置变更时序下提供更复杂的并行迁移协议。
- crash 恢复后不恢复旧的 RPC waiter，只恢复持久化状态并由 leader/recovery driver 重新推进。

## 线性一致性读

### 目标语义

普通 Raft 读如果也写入日志，可以自然获得线性一致性，但会增加一次复制成本。当前实现新增 lease/read-index 路径，目标是在保持线性一致性的前提下：

- leader 在 lease 有效时本地读。
- follower 可以通过 leader 提供的 read index 在本地读。
- 多个 follower read 可以 batching，共享同一个 read index。

旧版“Get 也写 Raft log”的实现保留为 `GetV1`，用于性能对比。

### Raft 负责什么

Raft 层只负责回答一个问题：当前 leader 能否给出一个安全的 read index。

实现集中在 `src/raft/raft.go`：

- `AppendEntriesAck` 表示某次 AppendEntries 是否被 follower 接受。
- `sendLogEntriesOnce()` 的原返回值仍只表示发送控制流是否停止/重试，不能当作 lease ack。
- heartbeat 路径创建 ack channel，并在 `HeartbeatAckTimeout` 内等待本轮 majority ack。
- 多数派确认后，用 heartbeat round 的 `sendTime + LeaderLeaseDuration` 推进 `leaderLeaseUntil`。
- `leaderLeaseUntil` 是 atomic timestamp，通过 CAS 做单调 max update。

关键安全假设：

- `LeaderLeaseDuration` 必须小于最小 election timeout，并预留调度/时钟误差余量。
- 当前代码中 election timeout 下界由 `ElectionTimeoutMinMillis` 给出，lease 长度由 `LeaderLeaseDuration` 给出。
- lease 起点使用 heartbeat round 发起时的 `sendTime`，不是最后一个 ack 到达时间；这样不会把 RPC 往返时间错误计入 lease window。

`LeaseReadIndex()` 只有在以下条件都满足时返回正数：

- 当前节点仍是 leader。
- 当前时间仍在 `leaderLeaseUntil` 之前。
- `commitIndex` 对应 current term entry，即当前 term 已经有 committed frontier。

返回值是 service-visible index，也就是 Raft 内部 `commitIndex + 1`。

### 为什么普通日志复制暂不刷新 lease

heartbeat 续 lease 是 round-based：这一轮从明确的 `sendTime` 开始，leader 自己算一票，收到同一轮 follower ack 后形成多数派证明。

普通日志复制是 stream-like：

- 每个 follower 的发送时刻不同。
- 重试和 backtracking 各自独立。
- 目标是最终复制成功，不是形成一轮清晰的 majority confirmation。

因此当前只让 heartbeat round 刷新 `leaderLeaseUntil`。成功的非-heartbeat AppendEntries 理论上也可以参与 lease 续期，但需要额外定义“分散 ack 如何组成一次合法 lease proof”的语义和聚合窗口；当前实现没有做这个优化。

### Heartbeat 提前触发

为了让 lease miss 尽快恢复，Raft 内部维护：

- `heartbeatSleepTo`
- `heartbeatWakeCh`

`wakeHeartbeat()` 会把下一次 heartbeat deadline 提前到当前时间，并非阻塞唤醒唯一 heartbeat goroutine。

调用点：

- `switchToLeader()`：新 leader 当选后尽快发出 heartbeat，建立 lease。
- `LeaseReadIndex()`：如果发现当前 leader lease 已过期，提前触发 heartbeat。

这条路径不会同步广播，也不会在 current-term commit 缺失时触发 heartbeat。current-term commit 缺失由上层 `Nop` 解决。

### ShardKV 负责什么

ShardKV 层不判断 quorum 或 leader lease，它只消费 Raft 给出的 read index，并保证本地业务状态已经可见。

leader `Get` 路径在 `src/shardkv/server.go` 中：

1. 调用 `getReadIndexImpl()`。
2. 如果 `LeaseReadIndex()` 失败，单次 RPC deadline 内最多提交一个 `Op{Op: Nop}`。
3. 等这个 no-op apply 后继续重试 read index。
4. 拿到 read index 后，等待 shardkv 自己的 `lastAppliedIndex >= readIndex`。
5. 把本地读投递到 `executeCh`，由 `stateMachineThread` 读取 `state/config/shardsToRecv`。

这里刻意不让 RPC goroutine 直接读业务状态。业务状态仍由状态机线程集中访问。

### Apply 等待和 executeCh

Raft 内部 `lastApplied` 不足以证明 ShardKV 状态机已经更新。因此 ShardKV 自己维护：

- `lastAppliedIndex`
- `applyNotifyCh`
- `applyMu`

每次状态机 apply 前进时，关闭当前 `applyNotifyCh` 并换成新 channel。等待方在锁内同时读取 `(lastAppliedIndex, applyNotifyCh)`，然后在锁外等待 channel close 或 deadline。

`applyCh` 只承载真正来自 Raft 的 `ApplyMsg`。本地任务统一走 `executeCh`，包括：

- `LocalReadReq`
- `SnapshotReq`

因此 `stateMachineThread` 对 `applyCh` 的正索引要求被收紧成不变量：`CommandIndex` 必须大于 0。

### Follower read 和 batching

Follower 不自己判断读是否安全。它先向 replica group 中的 leader 请求一个 safe read index：

- leader 侧 RPC：`GetReadIndex`
- 本地 helper：`getLeaderReadIndex()`
- leader 下标缓存：`lastLeaderIndex`

follower 收到 `Get` 且自己不是 leader 时，会把请求放进 `followerReadBatch`。batch flush 条件：

- batch 大小达到 `FollowerReadBatchSize`。
- 最早请求等待超过 `FollowerReadBatchWait`。

每个 batch 必须先冻结，再获取 read index。不能先拿 read index，再继续把后来的请求并入这一批。因为 read index 对应的线性化点必须晚于这一批请求的 arrival cutoff。

batch 拿到 read index 后：

1. follower 等本地 apply 到 read index。
2. 每个请求再通过 `executeCh` 完成本地读。
3. batch deadline 使用第一个请求的 `ArrivedAt + RPCTimeout`，因为 batch FIFO 且每个请求 timeout 固定。

### Client 侧缓存

`Clerk` 使用 atomic `lastLeader` 缓存最近成功服务请求的 server 下标。`Get/GetV1/PutAppend` 都从该位置开始环形尝试。

这个缓存只优化探测顺序，不改变正确性；失败时仍然遍历整个 group。

### Leader switch 后的语义

新 leader 当选后不能仅凭选票直接认为 lease read ready。选票证明它可以成为 leader，但不能替代 AppendEntries majority ack，也不能替代 current-term committed entry。

当前实现中，leader switch 后第一批读通常会承担恢复成本：

- `switchToLeader()` 提前唤醒 heartbeat 建立 lease。
- 第一次 read-index miss 可能触发 `Nop`，补 current-term committed frontier。
- 后续读恢复到 steady-state 本地读。

这不是长期活性下降，而是 first-read penalty。

对应测试：

- `TestLeaseReadOnLeader`
- `TestLeaseReadOnFollower`
- `TestLeaseReadAfterElection`

这些测试主要比较 RPC 数和请求耗时。由于 `labrpc.Network.GetTotalCount()` 是全网聚合统计，测试只断言相对关系，不把单次请求 RPC 数固定为某个精确常量。

常用验证命令：

- `cd src && go test -run '^$' ./raft`
- `cd src && go test -run '^$' ./shardkv`
- `cd src && go test -run 'TestLeaseReadOn(Follower|Leader)$' -count=1 ./shardkv`
- `cd src && go test -run '^TestLeaseReadAfterElection$' -count=1 ./shardkv`

### Kill 顺序

`ShardKV.Kill()` 是 teardown，不追求继续 drain Raft 中已经存在但尚未 apply 的命令。它的目标是先释放 ShardKV 层自己管理的等待者，再关闭底层依赖。

顺序约束：

- 先设置 `dead` 并关闭 `killCh`，让依赖 `kv.killed()` 或 `<-killCh` 的 goroutine 开始退出。
- 再唤醒 ShardKV 层事件源，包括 `condSendShards`、`applyNotifyCh`、`followerReadBatch` 和 pending client request。
- 然后停止 `mck` 和 `rf`。
- 最后调用 `CheckKillFinish()` 等待 ShardKV 自己的后台线程退出。

这里刻意让 `kv.rf.Kill()` 发生在 ShardKV 等待者被唤醒之后、`CheckKillFinish()` 之前。这样不会让上层 waiter 依赖一个已经关闭的 Raft 继续推进状态，也不会在退出检查时留下底层 Raft goroutine 继续运行。

### 当前没有继续做的细节

- 没有把普通日志复制成功也纳入 lease 续期证明。原因不是 AppendEntries 成功不能证明 follower 接受了 leader，而是普通复制路径缺少一个清晰的 round 语义。heartbeat 续约可以锚定为“某一轮从 `sendTime` 发出的广播，在同一 term 收到多数派 accepted ack”，于是 lease 起点可以保守地取这轮 `sendTime`。普通日志复制则是 per-follower stream：每个 follower 的发送时间、重试次数、backtracking 和 snapshot fallback 都可能不同。如果要把这些分散 ack 拼成 lease proof，就必须额外定义时间窗口、term 归属、self ack 归属，以及窗口起点如何取值。当前实现为了证明简单，只让 heartbeat round 刷新 lease。
- `LeaseReadIndex()` 没有暴露失败原因；上层只看到正数或失败。
- 没有为 follower read batch 做更复杂的 per-request deadline 分裂；当前使用 batch 中最早请求的 deadline。
- 没有引入固定 RPC 延迟的测试钩子；现有测试只使用 labrpc 已有的可靠/不可靠/long reordering 能力。

## 维护规则

- 设计结论变化时更新本文档。
- 已经完成的历史 TODO 不再保留为 TODO；改写成当前实现说明。
- 如果某个实现点被明确决定不做，记录“不做的原因”，不要只留下空泛待办。
