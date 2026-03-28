package coordinator

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/shardkv"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	clientId   int64
	seqCounter atomic.Int32
	dead       atomic.Bool
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.seqCounter.Store(0)
	ck.dead.Store(false)
	return ck
}

func (ck *Clerk) Kill() {
	ck.dead.Store(true)
}

func (ck *Clerk) killed() bool {
	return ck.dead.Load()
}

func (ck *Clerk) Transaction(txn_id string, ops []TxnOperation) []string {
	if len(ops) == 0 {
		return make([]string, 0)
	}
	args := &TxnArgs{}
	args.TxnId = txn_id
	args.Operations = ops
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply TxnReply
			ok := srv.Call("Coordinator.Transaction", args, &reply)
			if ok && reply.Err == shardkv.OK {
				return reply.Values
			}
			if ok && reply.Err == shardkv.ErrTxnAborted {
				return nil
			}
			// for other errors we should retry
			if ck.killed() {
				return nil
			}
		}
		// no active leader, sleep for re-election
		time.Sleep(100 * time.Millisecond)
	}
}
