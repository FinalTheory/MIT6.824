package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/kvraft"
	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func Key2shard(key string) int {
	return key2shard(key)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	clientId int64
	// Requests from the same clerk are ordered by seqCounter for dedup.
	// A clerk is therefore expected to be used serially; concurrent reuse
	// can cause older requests to become stale permanently.
	seqCounter atomic.Int32
	lastLeader atomic.Int32
}

func (ck *Clerk) Kill() {
	ck.sm.Kill()
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clientId = nrand()
	ck.seqCounter.Store(0)
	ck.lastLeader.Store(0)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	return ck.getImpl(key, "ShardKV.Get")
}

func (ck *Clerk) GetV1(key string) string {
	return ck.getImpl(key, "ShardKV.GetV1")
}

func (ck *Clerk) getImpl(key string, method string) string {
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		SeqNumber: ck.seqCounter.Add(1),
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			start := int(ck.lastLeader.Load())
			for offset := 0; offset < len(servers); offset++ {
				si := (start + offset) % len(servers)
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call(method, &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.lastLeader.Store(int32(si))
					return reply.Value
				}
				if ok && reply.Err == kvraft.ErrStaleRequest {
					panic("stale ShardKV clerk request: do not reuse one clerk concurrently")
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(ClerkRetryInterval)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op kvraft.OpType) {
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		SeqNumber: ck.seqCounter.Add(1),
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			start := int(ck.lastLeader.Load())
			for offset := 0; offset < len(servers); offset++ {
				si := (start + offset) % len(servers)
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.lastLeader.Store(int32(si))
					return
				}
				if ok && reply.Err == kvraft.ErrStaleRequest {
					panic("stale ShardKV clerk request: do not reuse one clerk concurrently")
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(ClerkRetryInterval)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
