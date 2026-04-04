package coordinator

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"6.5840/shardkv"
)

var configEndCounter atomic.Int64

type group struct {
	gid     int
	servers []*shardkv.ShardKV
	names   []string
}

type config struct {
	net         *labrpc.Network
	nservers    int
	ctrlerNames []string
	coordNames  []string
	ctrlers     []*shardctrler.ShardCtrler
	coords      []*Coordinator
	coordSaved  []*raft.Persister
	coordEnds   [][]string
	coordMEnds  [][]string
	groups      []*group
	smClerk     *shardctrler.Clerk
	kvClerk     *shardkv.Clerk
	coordClerk  *Clerk
	claimedKeys map[string]struct{}
}

func make_config(nservers int, gids ...int) *config {
	if len(gids) == 0 {
		gids = []int{100}
	}
	cfg := &config{
		net:         labrpc.MakeNetwork(),
		nservers:    nservers,
		ctrlerNames: make([]string, nservers),
		coordNames:  make([]string, nservers),
		ctrlers:     make([]*shardctrler.ShardCtrler, nservers),
		coords:      make([]*Coordinator, nservers),
		coordSaved:  make([]*raft.Persister, nservers),
		coordEnds:   make([][]string, nservers),
		coordMEnds:  make([][]string, nservers),
		groups:      make([]*group, len(gids)),
		claimedKeys: make(map[string]struct{}),
	}
	cfg.net.Reliable(true)

	for i := 0; i < nservers; i++ {
		cfg.ctrlerNames[i] = fmt.Sprintf("ctrler-%d", i)
		cfg.coordNames[i] = fmt.Sprintf("coord-%d", i)
	}
	for gi, gid := range gids {
		g := &group{
			gid:     gid,
			servers: make([]*shardkv.ShardKV, nservers),
			names:   make([]string, nservers),
		}
		for i := 0; i < nservers; i++ {
			g.names[i] = fmt.Sprintf("shard-%d-%d", gid, i)
		}
		cfg.groups[gi] = g
	}

	cfg.startCtrlers()
	cfg.smClerk = shardctrler.MakeClerk(cfg.makePeerEnds("ctrler-test", cfg.ctrlerNames))
	cfg.startShards()
	cfg.joinAllGroups()
	cfg.kvClerk = cfg.makeShardKVClerk()
	cfg.waitForShardKVReady()
	cfg.startCoordinators()
	cfg.coordClerk = cfg.makeCoordinatorClerk()
	return cfg
}

func (cfg *config) cleanup() {
	for _, co := range cfg.coords {
		co.Kill()
	}
	for _, g := range cfg.groups {
		for _, kv := range g.servers {
			kv.Kill()
		}
	}
	for _, ctrler := range cfg.ctrlers {
		ctrler.Kill()
	}
	cfg.coordClerk.Kill()
	cfg.kvClerk.Kill()
	cfg.smClerk.Kill()
	cfg.net.Cleanup()
}

func (cfg *config) endName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, configEndCounter.Add(1))
}

func (cfg *config) makeConnectedEnd(prefix string, servername string) *labrpc.ClientEnd {
	endname := cfg.endName(prefix)
	end := cfg.net.MakeEnd(endname)
	cfg.net.Connect(endname, servername)
	cfg.net.Enable(endname, true)
	return end
}

func (cfg *config) makePeerEnds(prefix string, serverNames []string) []*labrpc.ClientEnd {
	ends := make([]*labrpc.ClientEnd, len(serverNames))
	for i, serverName := range serverNames {
		ends[i] = cfg.makeConnectedEnd(prefix, serverName)
	}
	return ends
}

func (cfg *config) makeTrackedPeerEnds(prefix string, serverNames []string) ([]*labrpc.ClientEnd, []string) {
	ends := make([]*labrpc.ClientEnd, len(serverNames))
	names := make([]string, len(serverNames))
	for i, serverName := range serverNames {
		endname := cfg.endName(prefix)
		names[i] = endname
		ends[i] = cfg.net.MakeEnd(endname)
		cfg.net.Connect(endname, serverName)
		cfg.net.Enable(endname, true)
	}
	return ends, names
}

func (cfg *config) makeEnd(servername string) *labrpc.ClientEnd {
	return cfg.makeConnectedEnd("dynamic", servername)
}

func (cfg *config) startCtrlers() {
	for i := 0; i < cfg.nservers; i++ {
		cfg.ctrlers[i] = shardctrler.StartServer(
			cfg.makePeerEnds(fmt.Sprintf("ctrler-peer-%d", i), cfg.ctrlerNames),
			i,
			raft.MakePersister(),
		)

		srv := labrpc.MakeServer()
		srv.AddService(labrpc.MakeService(cfg.ctrlers[i]))
		srv.AddService(labrpc.MakeService(cfg.ctrlers[i].Raft()))
		cfg.net.AddServer(cfg.ctrlerNames[i], srv)
	}
}

func (cfg *config) startShards() {
	for _, g := range cfg.groups {
		for i := 0; i < cfg.nservers; i++ {
			g.servers[i] = shardkv.StartServer(
				cfg.makePeerEnds(fmt.Sprintf("shard-%d-peer-%d", g.gid, i), g.names),
				i,
				raft.MakePersister(),
				-1,
				g.gid,
				cfg.makePeerEnds(fmt.Sprintf("shard-%d-ctrler-%d", g.gid, i), cfg.ctrlerNames),
				cfg.makeEnd,
			)

			srv := labrpc.MakeServer()
			srv.AddService(labrpc.MakeService(g.servers[i]))
			srv.AddService(labrpc.MakeService(g.servers[i].Raft()))
			cfg.net.AddServer(g.names[i], srv)
		}
	}
}

func (cfg *config) startCoordinators() {
	for i := 0; i < cfg.nservers; i++ {
		cfg.startCoordinator(i)
	}
}

func (cfg *config) startCoordinator(i int) {
	if cfg.coordSaved[i] == nil {
		cfg.coordSaved[i] = raft.MakePersister()
	}
	peerEnds, peerNames := cfg.makeTrackedPeerEnds(fmt.Sprintf("coord-peer-%d", i), cfg.coordNames)
	ctrlerEnds, ctrlerNames := cfg.makeTrackedPeerEnds(fmt.Sprintf("coord-ctrler-%d", i), cfg.ctrlerNames)
	cfg.coordEnds[i] = peerNames
	cfg.coordMEnds[i] = ctrlerNames
	cfg.coords[i] = StartServer(
		peerEnds,
		i,
		cfg.coordSaved[i],
		ctrlerEnds,
		cfg.makeEnd,
	)
	srv := labrpc.MakeServer()
	srv.AddService(labrpc.MakeService(cfg.coords[i]))
	srv.AddService(labrpc.MakeService(cfg.coords[i].rf))
	cfg.net.AddServer(cfg.coordNames[i], srv)
}

func (cfg *config) shutdownCoordinator(i int) {
	for _, endname := range cfg.coordEnds[i] {
		cfg.net.Enable(endname, false)
	}
	for _, endname := range cfg.coordMEnds[i] {
		cfg.net.Enable(endname, false)
	}
	cfg.net.DeleteServer(cfg.coordNames[i])
	if cfg.coordSaved[i] != nil {
		cfg.coordSaved[i] = cfg.coordSaved[i].Copy()
	}
	if co := cfg.coords[i]; co != nil {
		co.Kill()
		cfg.coords[i] = nil
	}
}

func (cfg *config) joinAllGroups() {
	servers := make(map[int][]string, len(cfg.groups))
	for _, g := range cfg.groups {
		servers[g.gid] = append([]string(nil), g.names...)
	}
	cfg.smClerk.Join(servers)
}

func (cfg *config) makeShardKVClerk() *shardkv.Clerk {
	return shardkv.MakeClerk(cfg.makePeerEnds("kv-ctrler", cfg.ctrlerNames), cfg.makeEnd)
}

func (cfg *config) makeCoordinatorClerk() *Clerk {
	return MakeClerk(cfg.makePeerEnds("coord-client", cfg.coordNames))
}

func (cfg *config) claimKeyInGID(gid int) string {
	prefixForShard := func(shard int) byte {
		for ch := byte('a'); ch <= byte('z'); ch++ {
			if int(ch)%shardctrler.NShards == shard {
				return ch
			}
		}
		panic("no prefix for shard")
	}
	for {
		config := cfg.smClerk.Query(-1)
		for shard, group := range config.Shards {
			if group == gid {
				prefix := string(prefixForShard(shard))
				for suffix := 0; ; suffix++ {
					key := prefix
					if suffix > 0 {
						key = fmt.Sprintf("%s%d", prefix, suffix)
					}
					if _, ok := cfg.claimedKeys[key]; ok {
						continue
					}
					cfg.claimedKeys[key] = struct{}{}
					return key
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (cfg *config) waitForShardKVReady() {
	for _, g := range cfg.groups {
		key := cfg.claimKeyInGID(g.gid)
		value := fmt.Sprintf("ready-%d", g.gid)
		cfg.kvClerk.Put(key, value)
		if got := cfg.kvClerk.Get(key); got != value {
			panic("shardkv not ready")
		}
	}
}

func (cfg *config) runTxn(t *testing.T, ck *Clerk, txnID string, ops []TxnOperation, want []string) {
	done := make(chan []string, 1)
	go func() { done <- ck.Transaction(txnID, ops) }()
	select {
	case got := <-done:
		if len(got) != len(want) {
			t.Fatalf("%s returned unexpected length: %v", txnID, got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s result[%d] mismatch: got %q want %q", txnID, i, got[i], want[i])
			}
		}
	case <-time.After(8 * time.Second):
		t.Fatalf("%s timed out", txnID)
	}
}

func callGroupRPC[T any](t *testing.T, cfg *config, gid int, rpcName string, args any, getErr func(*T) shardkv.Err) T {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, g := range cfg.groups {
			if g.gid != gid {
				continue
			}
			for _, name := range g.names {
				var reply T
				if srv := cfg.makeEnd(name); srv.Call(rpcName, args, &reply) && getErr(&reply) != shardkv.ErrWrongLeader {
					return reply
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no shardkv leader for gid %d", gid)
	var zero T
	return zero
}

func (cfg *config) callGroupPrepare(t *testing.T, gid int, args *shardkv.PrepareArgs) shardkv.PrepareReply {
	return callGroupRPC(t, cfg, gid, "ShardKV.Prepare", args, func(reply *shardkv.PrepareReply) shardkv.Err { return reply.Err })
}

func (cfg *config) callGroupCommit(t *testing.T, gid int, args *shardkv.CommitArgs) shardkv.CommitReply {
	return callGroupRPC(t, cfg, gid, "ShardKV.Commit", args, func(reply *shardkv.CommitReply) shardkv.Err { return reply.Err })
}

func (cfg *config) callGroupAbort(t *testing.T, gid int, args *shardkv.AbortArgs) shardkv.AbortReply {
	return callGroupRPC(t, cfg, gid, "ShardKV.Abort", args, func(reply *shardkv.AbortReply) shardkv.Err { return reply.Err })
}
