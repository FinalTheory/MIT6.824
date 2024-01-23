package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	ConfigChange = "ConfigChange"
	InstallShard = "InstallShard"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int64
	SeqNumber int32
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientId  int64
	SeqNumber int32
}

type GetReply struct {
	Err   Err
	Value string
}

type InstallShardArgs struct {
	Shard int
	Data  map[string]string
	Num   int
	// for logging purpose
	From int
}

type InstallShardReply struct {
	Success bool
}

type ShardInfo struct {
	Shard int
	Num   int
}

func (args *InstallShardArgs) ShardInfo() ShardInfo {
	return ShardInfo{Shard: args.Shard, Num: args.Num}
}
