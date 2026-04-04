# 2PC 设计说明

## 文档目的

这份文档用于统一记录：

- 2PC 协议希望具备的语义
- 当前已经选定的实现方案
- 已知缺口与 TODO

后续只要实现发生变化，这份文档也应该一起更新。

## 当前范围

- 当前项目是在已有的 `shardkv` 和 Raft 之上，手工实现一个经典 2PC 风格的事务层。
- 当前目标是先把 coordinator 一侧的协议形状理顺，再逐步补 participant 一侧的真实语义。
- 设计上应尽量避免干扰已经正确的 `shardkv` 主逻辑。

## 当前 Coordinator 接口

- 客户端入口：`Clerk.Transaction(txnID, ops) []string`
- RPC 入口：`Coordinator.Transaction(args, reply)`
- 当前计划中的返回语义是：
- 返回的 slice 长度必须与输入 `ops` 完全一致
- 返回结果的每个位置都与输入中同位置的操作一一对应
- 对于 `Get`，返回读到的值
- 对于 `Put`，返回写入的值
- 对于 `Append`，当前实现返回追加后的最终值
- 如果事务 abort，则当前客户端接口返回 `nil`

## 已选定的 Coordinator 状态机

- `Prepare`
- `Commit`
- `Abort`
- `Committed`
- `Aborted`

当前打算赋予它们的含义：

- `Prepare`：coordinator 已经把参与者集合持久化，并开始向所有 participant group 发送 prepare
- `Commit`：coordinator 已经做出了 commit 决议，正在传播这个决议
- `Abort`：coordinator 已经做出了 abort 决议，正在传播这个决议
- `Committed` 与 `Aborted`：最终返回给客户端的终态

重要协议约束：

- 一旦 coordinator 决定进入 `Commit`，之后就不能再回退成 `Abort`
- 一旦 coordinator 决定进入 `Abort`，之后也不能再回到 `Commit`

## 当前已选定的 Coordinator 实现方案

- coordinator 先通过 Raft 持久化事务状态，再进入下一步
- 事务开始时只计算一次 participant 集合，并和初始的 `Prepare` 命令一起持久化
- coordinator 在按 participant 分组事务操作时，除了保存每个 participant 对应的 `TxnOperation` 子序列，还会同时持久化这些子操作在原始事务中的下标；这样后续收集各 participant 的 `CommitReply.Values` 时，能够重新拼回与客户端输入顺序一致的结果 slice
- coordinator 在事务最终进入 `Committed` 时，应把拼接后的最终 `Values` 一起写入自己的状态机；这样同一事务的后续重试或重复查询可以稳定返回同一份结果，而不是依赖某次临时 RPC 收集结果
- 一笔事务会绑定到启动时捕获的某个特定 shard 配置
- `Commit` 和 `Abort` 都被建模为显式的中间状态，而不是直接跳到终态
- 当前把“调用 `Start`、等待 apply、超时重试、遇到非 leader 时退出”这段重复逻辑提炼成了通用 helper `PersistCommand`
- `executeAction` 会在仍然是 leader 时持续重试传播；一旦失去领导权则停止
- `Kill()` 现在会等待 coordinator executor 和 config puller 两个 goroutine 退出

## 基于 Channel 的提交确认语义

当前代码中，状态推进大量依赖“命令自带 channel，等待 apply 后唤醒”这套模式。

这套机制当前打算表达的语义是：

- 只有当某个状态推进命令已经真正进入状态机并 apply 之后，等待方才认为这次推进成功
- 如果 `rf.Start(cmd)` 返回 `isLeader == true`，那么当前这次 RPC / goroutine 可以等待这条命令自带的 channel
- 这个 channel 只服务于“当前进程内的一次提交确认”，不承担崩溃恢复责任
- 如果 leader 在等待期间 crash 或重启，那么之前等待的 channel 自然失效；这是这套机制的预期边界，不是额外 bug

这套模式目前适合：

- 在单次存活周期内，确认“我刚刚发起的这条状态推进命令是否已经 apply”
- 因为这套模式在 coordinator 和 participant 两侧都会重复出现，所以当前代码里引入了 `PersistCommand` 作为共享模板

`PersistCommand` 当前表达的语义是：

- 调用 `rf.Start(cmd)`
- 如果发现自己已经不是 leader，则执行调用方提供的 `notLeaderCallback` 并退出
- 如果当前仍是 leader，则等待对应的 result channel 被 apply 一侧唤醒
- 如果等待超时，则继续重试，直到成功或发现自己已经失去领导权

对调用方来说：

- 成功返回表示“这条命令已经在当前进程生命周期内被状态机 apply 过”
- 失败返回表示“当前节点在这次等待过程中失去了推进这条命令的资格”，而不是协议层上的最终失败

这套模式目前不负责：

- 崩溃恢复后重新找回之前的 waiter
- 通过 channel 本身实现持久化恢复语义

## 为什么等待不能无限阻塞

如果一个节点在 `Start()` 时看到自己是 leader，然后无条件一直等待成功 channel，那么在等待期间失去领导权时，这次 RPC 可能永久挂起。

典型原因包括：

- 这条日志后来根本没有被提交
- 原先的 leader 在等待期间失去领导权
- 同一个逻辑状态最后由别的 leader 推进成功，但当前 waiter 永远等不到自己的 channel

因此，这类等待必须始终带有失败出口。当前讨论收敛出的原则是：

- 不能只靠成功 channel
- 至少要有 timeout 作为兜底
- 更完整的做法是同时具备：
- success channel：命令真正 apply
- fail channel：失去领导权、term 变化、index 被覆盖等
- timeout：最后兜底，避免永久挂起

就目前阶段而言：

- coordinator 侧已经采用了“等待 + timeout + 重试/退出”这一类思路
- participant 侧 `Prepare/Commit/Abort` 最终也应遵守同样的原则，避免因为 leader 在等待期间失去领导权而把 RPC 永久挂住

## 期望中的 Participant RPC 语义

- `Prepare`、`Commit`、`Abort` 都应该支持幂等
- 如果 participant 本地已经有足够新的已应用事务状态，那么可以直接返回 `OK`
- 这个 fast path 必须依赖“已经进入状态机并已应用的状态”，不能依赖临时内存变量
- 对于 `Prepare`，如果不能命中 fast path，则必须通过 participant group 自己的 Raft 日志落地，并且只有在 apply 之后才能返回 `OK`
- 对于 `Commit`，如果 participant 已经处于 committed 状态，应直接返回 `OK`
- 对于 `Abort`，如果 participant 已经处于 aborted 状态，应直接返回 `OK`
- 当前实现里，participant 已经具备基础的 `Prepare/Commit/Abort` 状态机与幂等行为，并且 `Commit` 会把最终 `Values` 持久化进 `TxnState`

## 事务内读操作的语义

- 当前 txn 内部已经有独立的读写锁与冲突检查；但普通 `Get/Put/Append` 仍不与 txn 共用同一套锁表，因此系统整体并未收敛成统一的完整事务化访问模型
- 如果后续决定让 txn 内的读真正参与事务语义，那么在单个 shard 内需要对读 key 和写 key 都施加正确的读写锁，并在事务终态前不释放
- 但当前设计明确不考虑让普通 `Get/Put/Append` 请求与 txn 共享同一套锁逻辑，以避免实现过度复杂化，并尽量不为现有 shardkv 正确路径引入 bug

## Reconfiguration 交互问题

当前设计对 reconfiguration 的处理采用一个明确的简化语义：

- 不能通过“允许 `Commit` 逻辑失败，再把事务改判成 `Abort`”来绕过这个问题，因为这会直接破坏事务原子性

目前讨论出的较可行简化路线是：

- prepared txn 可以暂缓其所涉及 shard 的 config apply
- 更准确地说，是“延后应用相关的 config change”，而不是“阻止 shardctrler 发布配置”
- 旧 owner 在最终决议送达之前，应仍然能够继续处理这笔 prepared txn 的 `Commit` 或 `Abort`
- 这里的阻塞条件不应粗暴定义成“只要当前 group 上存在任何 read/write set 就一律阻塞 config update”
- 更精确的规则应该是：只有当某个未决 txn 的读写集合与本次 config change 实际涉及的 shard 有交集时，才延后这次 config apply
- 对无关 shard 的 prepared txn，不应阻塞无关 shard 的配置推进

如果采用“延后 config apply”这条路：

- 延后应尽量精确到“涉及本次迁移 shard 的 prepared txn”
- 无关事务不应阻塞无关 shard 的 config 推进
- 被延后的 config change 需要后续继续重试；当前代码里这可以由 config puller 的周期性拉取自然触发，不一定需要额外新增一套显式重试机制
- 对应 txn 在 `Commit/Abort` 后释放锁时，也应能重新触发这些被延后的配置推进
- 当前代码已经接入一个最小版本的守卫：`isConfigChangeValid` 会在原有“配置号单调推进且没有待接收 shard”的基础上，再检查未决 txn 的读写锁集合是否命中本次 `shardsToSend`；如果命中，则先延后这次 config apply
- 当前设计就收敛到这一层，不再额外收紧 participant 事务 RPC 的 config 合法性检查；participant 仍然保留最基本的 `Prepare` 配置号匹配检查，因此像“config 先变、prepare 后到”这种时序会直接因 config num 不匹配而拒绝，不再单独作为待解决正确性问题

## 活性与恢复

- coordinator 侧当前也已经接入基础的 snapshot / reload：直接持久化 `stateTable`，其中事务状态、分组信息和最终 `Values` 会随快照一起恢复；`ResultCh` 这类进程内临时通知器在恢复后不会被反序列化回来，代码应把它视为可能为 `nil` 的临时字段并显式处理。快照触发方式也已经收敛为：根据 `persister.RaftStateSize()` 超过阈值后再做，而不是每条命令都 snapshot
- `shardkv` participant 侧当前已经将 `txnStateMap`、`txnReadSet`、`txnWriteSet` 一并纳入 snapshot / reload；也就是说，prepared txn 的本地事务状态和锁表现在会随快照一起持久化与恢复
- coordinator 侧当前已经接入一个最小版 recovery driver：leader 会定期扫描 `Prepare/Commit/Abort` 三种中间态事务，并在没有后台 worker 正在推进该 txn 时重新启动 `executePrepare/ensureFinalAction`
- recovery driver 自身不直接执行状态机，因此不会立刻补建中间态 txn 的 `ResultCh`；这没有问题，因为恢复路径的目标是重新推进协议，而不是恢复旧 waiter。后续状态再次经由 Raft apply 时，状态机再按需要建立和使用新的 `ResultCh` 即可
- 当前设计依赖 coordinator 侧的高可用与恢复推进：只要 coordinator 集群最终能恢复 leader，participant 不需要额外实现“主动查询最终决议”这一套收尾机制；participant 只需要在重启后保留 prepared 状态，并继续被动接受后续 `Commit/Abort`
- 即使采用“prepared txn 延后迁移”的简化方案，仅凭有界延迟网络也不足以证明系统活性

## 尽量避免干扰现有 shardkv

以下是实现时需要遵守的原则：

- 普通 `Get/Put/Append` 路径尽量保持不动
- 事务状态应放在独立数据结构里，不要复用普通 KV dedup 状态
- prepared 但尚未提交的事务数据不应污染已经提交的 KV 状态
- 事务 RPC 的特殊语义应与普通 KV RPC 语义隔离
- 优先选择“新增 RPC + 少量 config apply 守卫”，而不是大规模侵入已有 shard migration 主流程
- 现有普通 KV 请求的 `pendingRequests / failConflictPendingRequests` 机制应只作用于原有 `Op` 命令，不应直接套到 `TxnOp` 上
- 将 `failConflictPendingRequests(cmd)` 下沉到 `case Op:` 分支内，可以避免 `TxnOp` 被错误断言成 `Op` 而 panic；在当前实现里，这样做原则上不会破坏原有 KV 正确性
- 更精确地说，这种移动主要可能影响的是“等待中的普通 KV 请求在遇到非 Op 冲突时是否能立刻失败返回”；即使没有这层快速失败，现有 timeout 与 term-change fail path 仍然可以兜底，因此更像响应性退化，而不是原有 KV 语义错误
- 需要记住：Raft log index 是所有命令类型共享的一维空间，不能阻止 `TxnOp` 落到原本某个等待中的 KV `Op` 所使用的 index 上
- 因此，从长远看，冲突检测的正确抽象不是“只处理 Op”，而是“任何提交到同一 index 的其他命令类型，都可能让等待该 index 的普通 KV 请求需要失败返回”
- 对普通 KV 请求来说，更根本的判断应是：“最终提交到这个 index 的命令，是否就是这个 waiter 正在等待的那条请求”；如果不是，无论它是另一个 `Op`、`TxnOp`，还是未来别的命令类型，都应视为冲突并失败返回

## 当前 TODO

### 1. Reconfiguration 交互

- 如果 prepared txn 可以延后 config apply，那么“哪些 config change 会被阻塞”的具体判定规则还可以继续细化；当前最小实现已经做到：只要未决 txn 的读写锁集合命中本次 `shardsToSend`，就延后该次 config apply

### 2. 测试覆盖

当前已经补上的测试大致分为三层：

- 基础 correctness：基本跨 group 成功路径、幂等 retry、空事务 / 单 participant / 单 key 单操作边界、同一 `txnID` 重复调用时以第一次结果为准
- 定向协议语义：冲突 abort、同 key 多次操作顺序与返回值、`Abort` 先到后的 tombstone、participant commit 幂等、triangle conflict、`Prepare` 挡住 reconfig、部分 participant 已 prepared 时的全局 abort
- 随机扰动回归：统一的随机事务框架支持 `unreliable`、`long reordering`、coordinator crash/restart 与动态 reconfig 的组合开关；worker 间不共享 key，因此可以持续做强最终值检查。随机测试还额外接入了一套轻量事件 recorder，用来确认诸如 not-leader、group RPC failure、recovery driver 与 snapshot save/load 等关键路径确实被触发

随机测试目前保持一个刻意的限制：

- 不让不同 worker 共享 key。这样可以把复杂度集中在网络、coordinator 恢复与 reconfig 扰动本身，同时保留强最终状态校验；随机冲突语义已经由前面的定向 case 单独覆盖

#### 可以主要依赖随机 crash / unreliable network 覆盖的场景

以下测试运行期间要不停进行reconfig，并且最终需要打点确认逻辑分支真的有走到。

- coordinator 在事务中间态附近 crash/restart，随后由 recovery driver 继续推进到终态
- snapshot 真正触发后的恢复路径测试：coordinator / participant 在快照后重启，事务状态、锁状态与最终 `Values` 仍一致
- unreliable network 下的幂等测试：`Prepare/Commit/Abort` 请求或 reply 丢失后重试，participant 仍保持幂等，`Commit` 重试仍返回同样的 `Values`
- unreliable network 下的部分送达测试：只有部分 participant 收到 `Prepare/Commit/Abort`，随后通过重试或 leader 接手最终收敛到全局一致结果
- unreliable network 叠加 coordinator leader 切换：旧 leader 只完成部分 RPC 发送后失效，新 leader 通过 recovery driver 继续推进未决事务
- client 到 coordinator 的事务 reply 丢失测试：客户端以同一 `txnID` 重试 `Transaction` 时，最终返回结果保持一致
- coordinator leader 在事务中途切换，但客户端只通过同一 `txnID` 做无感重试，最终仍返回一致结果

#### 单独保留的高层语义测试

- 事务模型检查：后续可为 coordinator 的 `Transaction(txnID, ops) -> []string/nil` 增加一层基于 history 的线性化测试；当前只计划检查事务接口层能否被解释为某个合法的线性化顺序，不进一步扩展成更一般的串行化/隔离级别验证

## 维护规则

- 每当某个设计讨论已经收敛，就更新这份文档
- 如果某个 TODO 被确认已经完成，就直接删除或改写对应条目，避免重复堆积
