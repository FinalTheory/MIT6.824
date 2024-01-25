package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

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

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.ClientId = ck.clientId
	args.SeqNumber = ck.seqCounter.Add(1)
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
			if ck.killed() {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	args.ClientId = ck.clientId
	args.SeqNumber = ck.seqCounter.Add(1)
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			if ck.killed() {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	args.ClientId = ck.clientId
	args.SeqNumber = ck.seqCounter.Add(1)
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			if ck.killed() {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	args.ClientId = ck.clientId
	args.SeqNumber = ck.seqCounter.Add(1)
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			if ck.killed() {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
