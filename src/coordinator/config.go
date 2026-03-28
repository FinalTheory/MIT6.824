package coordinator

import (
	"fmt"
	"sync/atomic"
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
	groups      []*group
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
		groups:      make([]*group, len(gids)),
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
	cfg.startShards()
	time.Sleep(500 * time.Millisecond)
	cfg.joinAllGroups()
	cfg.startCoordinators()
	time.Sleep(800 * time.Millisecond)
	return cfg
}

func (cfg *config) cleanup() {
	for _, co := range cfg.coords {
		co.Kill()
	}
	for _, g := range cfg.groups {
		for _, shard := range g.servers {
			shard.Kill()
		}
	}
	for _, ctrler := range cfg.ctrlers {
		ctrler.Kill()
	}
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
		cfg.coords[i] = StartServer(
			cfg.makePeerEnds(fmt.Sprintf("coord-peer-%d", i), cfg.coordNames),
			i,
			raft.MakePersister(),
			cfg.makePeerEnds(fmt.Sprintf("coord-ctrler-%d", i), cfg.ctrlerNames),
			cfg.makeEnd,
		)

		srv := labrpc.MakeServer()
		srv.AddService(labrpc.MakeService(cfg.coords[i]))
		srv.AddService(labrpc.MakeService(cfg.coords[i].rf))
		cfg.net.AddServer(cfg.coordNames[i], srv)
	}
}

func (cfg *config) joinAllGroups() {
	ck := shardctrler.MakeClerk(cfg.makePeerEnds("ctrler-join", cfg.ctrlerNames))
	defer ck.Kill()
	servers := make(map[int][]string, len(cfg.groups))
	for _, g := range cfg.groups {
		servers[g.gid] = append([]string(nil), g.names...)
	}
	ck.Join(servers)
}

func (cfg *config) makeShardKVClerk() *shardkv.Clerk {
	return shardkv.MakeClerk(cfg.makePeerEnds("kv-ctrler", cfg.ctrlerNames), cfg.makeEnd)
}

func (cfg *config) makeCoordinatorClerk() *Clerk {
	return MakeClerk(cfg.makePeerEnds("coord-client", cfg.coordNames))
}
