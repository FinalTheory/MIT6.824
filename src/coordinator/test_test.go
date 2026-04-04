package coordinator

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"6.5840/kvraft"
	"6.5840/porcupine"
	"6.5840/shardctrler"
	"6.5840/shardkv"
)

const linearizabilityCheckTimeout = 1 * time.Second

type txnOpLog struct {
	sync.Mutex
	operations []porcupine.Operation
}

func (log *txnOpLog) Append(op porcupine.Operation) {
	log.Lock()
	defer log.Unlock()
	log.operations = append(log.operations, op)
}

func (log *txnOpLog) Read() []porcupine.Operation {
	log.Lock()
	defer log.Unlock()
	ops := make([]porcupine.Operation, len(log.operations))
	copy(ops, log.operations)
	return ops
}

func TestTxnBasicCases(t *testing.T) {
	cfg := make_config(3, 100, 101)
	defer cfg.cleanup()

	t.Run("nil", func(t *testing.T) {
		cfg.runTxn(t, cfg.coordClerk, "txn-empty", nil, []string{})
	})

	t.Run("empty", func(t *testing.T) {
		cfg.runTxn(t, cfg.coordClerk, "txn-empty", []TxnOperation{}, []string{})
	})

	t.Run("single-participant", func(t *testing.T) {
		readKey := cfg.claimKeyInGID(100)
		writeKey := cfg.claimKeyInGID(100)
		cfg.kvClerk.Put(readKey, "seed")
		cfg.runTxn(t, cfg.coordClerk, "txn-single-group", []TxnOperation{
			{Key: readKey, Op: kvraft.GetOp},
			{Key: writeKey, Value: "value", Op: kvraft.PutOp},
		}, []string{"seed", "value"})
		if got := cfg.kvClerk.Get(writeKey); got != "value" {
			t.Fatalf("single-participant write mismatch: got %q", got)
		}
	})

	t.Run("single-key-single-op", func(t *testing.T) {
		key := cfg.claimKeyInGID(101)
		cfg.runTxn(t, cfg.coordClerk, "txn-single-op", []TxnOperation{
			{Key: key, Value: "once", Op: kvraft.PutOp},
		}, []string{"once"})
		if got := cfg.kvClerk.Get(key); got != "once" {
			t.Fatalf("single-op write mismatch: got %q", got)
		}
	})

	t.Run("same-txnid-different-ops", func(t *testing.T) {
		readKey := cfg.claimKeyInGID(100)
		firstWriteKey := cfg.claimKeyInGID(101)
		secondWriteKey := cfg.claimKeyInGID(101)
		cfg.kvClerk.Put(readKey, "seed")
		firstOps := []TxnOperation{
			{Key: readKey, Op: kvraft.GetOp},
			{Key: firstWriteKey, Value: "first", Op: kvraft.PutOp},
		}
		secondOps := []TxnOperation{
			{Key: readKey, Op: kvraft.GetOp},
			{Key: secondWriteKey, Value: "second", Op: kvraft.PutOp},
		}
		want := []string{"seed", "first"}
		cfg.runTxn(t, cfg.coordClerk, "txn-same-id", firstOps, want)
		cfg.runTxn(t, cfg.coordClerk, "txn-same-id", secondOps, want)
		if got := cfg.kvClerk.Get(firstWriteKey); got != "first" {
			t.Fatalf("same-txnid first write mismatch: got %q", got)
		}
		if got := cfg.kvClerk.Get(secondWriteKey); got != "" {
			t.Fatalf("same-txnid second write should be ignored: got %q", got)
		}
	})

	t.Run("same-txnid-same-ops", func(t *testing.T) {
		readKey := cfg.claimKeyInGID(100)
		writeKey := cfg.claimKeyInGID(101)
		cfg.kvClerk.Put(readKey, "seed")
		ops := []TxnOperation{
			{Key: readKey, Op: kvraft.GetOp},
			{Key: writeKey, Value: "value", Op: kvraft.PutOp},
		}
		got1 := cfg.coordClerk.Transaction("txn-idempotent", ops)
		got2 := cfg.coordClerk.Transaction("txn-idempotent", ops)
		if !reflect.DeepEqual(got1, []string{"seed", "value"}) || !reflect.DeepEqual(got1, got2) {
			t.Fatalf("unexpected idempotent txn results: %v %v", got1, got2)
		}
		if got := cfg.kvClerk.Get(writeKey); got != "value" {
			t.Fatalf("writeKey mismatch after idempotent retry: got %q", got)
		}
	})
}

func TestTxnRepeatedKeyOps(t *testing.T) {
	cfg := make_config(3, 100, 101, 102, 103, 104)
	defer cfg.cleanup()

	keys := []string{
		cfg.claimKeyInGID(100),
		cfg.claimKeyInGID(101),
		cfg.claimKeyInGID(102),
		cfg.claimKeyInGID(103),
		cfg.claimKeyInGID(104),
	}
	build := func(round int, count int) ([]TxnOperation, []string, map[string]string) {
		r := rand.New(rand.NewSource(int64(round + 1)))
		state := map[string]string{}
		for i, key := range keys {
			state[key] = "base-" + strconv.Itoa(i) + "-" + strconv.Itoa(round)
		}
		ops, want := make([]TxnOperation, 0, count), make([]string, 0, count)
		for i := 0; i < count; i++ {
			key := keys[r.Intn(len(keys))]
			switch r.Intn(3) {
			case 0:
				ops = append(ops, TxnOperation{Key: key, Op: kvraft.GetOp})
				want = append(want, state[key])
			case 1:
				value := "put-" + strconv.Itoa(round) + "-" + strconv.Itoa(i)
				ops = append(ops, TxnOperation{Key: key, Value: value, Op: kvraft.PutOp})
				state[key] = value
				want = append(want, value)
			default:
				value := "+app-" + strconv.Itoa(round) + "-" + strconv.Itoa(i)
				ops = append(ops, TxnOperation{Key: key, Value: value, Op: kvraft.AppendOp})
				state[key] += value
				want = append(want, state[key])
			}
		}
		return ops, want, state
	}
	for round := 0; round < 10; round++ {
		for i, key := range keys {
			cfg.kvClerk.Put(key, "base-"+strconv.Itoa(i)+"-"+strconv.Itoa(round))
		}
		ops, want, state := build(round, 30)
		cfg.runTxn(t, cfg.coordClerk, "txn-repeated-key-"+strconv.Itoa(round), ops, want)
		for _, key := range keys {
			if got := cfg.kvClerk.Get(key); got != state[key] {
				t.Fatalf("round %d: %s mismatch: got %q want %q", round, key, got, state[key])
			}
		}
	}
}

func TestTxnConflictAbort(t *testing.T) {
	cfg := make_config(3, 100, 101)
	defer cfg.cleanup()

	lockKey := cfg.claimKeyInGID(100)
	otherKey := cfg.claimKeyInGID(100)
	thirdKey := cfg.claimKeyInGID(101)
	for round := 0; round < 20; round++ {
		suffix := strconv.Itoa(round)
		holderID := "txn-lock-holder-" + suffix
		cfg.kvClerk.Put(lockKey, "old-lock-"+suffix)
		cfg.kvClerk.Put(otherKey, "old-other-"+suffix)
		cfg.kvClerk.Put(thirdKey, "old-third-"+suffix)
		config := cfg.smClerk.Query(-1)
		if reply := cfg.callGroupPrepare(t, 100, &shardkv.PrepareArgs{
			TxnId:  holderID,
			Config: config,
			TxnOperations: []shardkv.TxnOperation{
				{Key: lockKey, Op: kvraft.GetOp},
				{Key: lockKey, Value: "held-" + suffix, Op: kvraft.PutOp},
			},
		}); reply.Err != shardkv.OK {
			t.Fatalf("round %d: prepare lock holder failed: %v", round, reply.Err)
		}
		abortCh := make(chan []string, 1)
		successCh := make(chan []string, 1)
		go func() {
			abortCh <- cfg.coordClerk.Transaction("txn-should-abort-"+suffix, []TxnOperation{
				{Key: lockKey, Op: kvraft.GetOp},
				{Key: otherKey, Value: "new-other-" + suffix, Op: kvraft.PutOp},
			})
		}()
		go func() {
			successCh <- cfg.coordClerk.Transaction("txn-should-succeed-"+suffix, []TxnOperation{
				{Key: otherKey, Value: "new-other-2-" + suffix, Op: kvraft.PutOp},
				{Key: thirdKey, Value: "new-third-" + suffix, Op: kvraft.PutOp},
			})
		}()
		if got := <-abortCh; got != nil {
			t.Fatalf("round %d: conflicting txn should abort, got %v", round, got)
		}
		if got := <-successCh; !reflect.DeepEqual(got, []string{"new-other-2-" + suffix, "new-third-" + suffix}) {
			t.Fatalf("round %d: independent txn should succeed, got %v", round, got)
		}
		if got := cfg.kvClerk.Get(lockKey); got != "old-lock-"+suffix {
			t.Fatalf("round %d: lockKey changed after aborted txn: %q", round, got)
		}
		if got := cfg.kvClerk.Get(otherKey); got != "new-other-2-"+suffix {
			t.Fatalf("round %d: otherKey mismatch after concurrent txns: %q", round, got)
		}
		if got := cfg.kvClerk.Get(thirdKey); got != "new-third-"+suffix {
			t.Fatalf("round %d: thirdKey mismatch after successful txn: %q", round, got)
		}
		if reply := cfg.callGroupAbort(t, 100, &shardkv.AbortArgs{TxnId: holderID}); reply.Err != shardkv.OK {
			t.Fatalf("round %d: abort lock holder failed: %v", round, reply.Err)
		}
	}
}

func TestTxnPrepareBlocksReconfig(t *testing.T) {
	cfg := make_config(3, 100, 101)
	defer cfg.cleanup()

	key := cfg.claimKeyInGID(100)
	cfg.kvClerk.Put(key, "base")
	config := cfg.smClerk.Query(-1)
	if reply := cfg.callGroupPrepare(t, 100, &shardkv.PrepareArgs{
		TxnId:         "txn-reconfig-hold",
		Config:        config,
		TxnOperations: []shardkv.TxnOperation{{Key: key, Value: "held", Op: kvraft.PutOp}},
	}); reply.Err != shardkv.OK {
		t.Fatalf("prepare before reconfig failed: %v", reply.Err)
	}
	cfg.smClerk.Leave([]int{100})
	if moved := cfg.smClerk.Query(-1).Shards[shardkv.Key2shard(key)]; moved != 101 {
		t.Fatalf("shardctrler did not move shard to gid 101, got %d", moved)
	}
	// Use a fresh clerk so it routes from the controller's latest config
	// immediately; this lets us observe that reconfig appears blocked from the
	// client side while the prepared txn is still holding the old shard owner.
	probe := cfg.makeShardKVClerk()
	defer probe.Kill()
	done := make(chan struct{}, 1)
	go func() {
		probe.Put(key, "after-reconfig")
		done <- struct{}{}
	}()
	select {
	case <-done:
		t.Fatal("reconfig should stay blocked while prepared txn is still holding the shard")
	case <-time.After(1 * time.Second):
	}
	if reply := cfg.callGroupAbort(t, 100, &shardkv.AbortArgs{TxnId: "txn-reconfig-hold"}); reply.Err != shardkv.OK {
		t.Fatalf("abort after reconfig failed: %v", reply.Err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reconfig did not resume after abort released the prepared txn")
	}
	if got := probe.Get(key); got != "after-reconfig" {
		t.Fatalf("reconfig probe mismatch after unblock: got %q", got)
	}
}

func TestTxnParticipantCrashRecovery(t *testing.T) {
	resetEvents()
	cfg := make_config(3, 100, 101)
	defer cfg.cleanup()

	keyA := cfg.claimKeyInGID(100)
	keyB := cfg.claimKeyInGID(101)
	cfg.kvClerk.Put(keyA, "base-a")
	cfg.kvClerk.Put(keyB, "base-b")
	config := cfg.smClerk.Query(-1)
	txnID := "txn-crash-reconfig"
	ops := []TxnOperation{
		{Key: keyA, Value: "held-a", Op: kvraft.PutOp},
		{Key: keyB, Value: "held-b", Op: kvraft.PutOp},
	}
	if reply := cfg.callGroupPrepare(t, 100, &shardkv.PrepareArgs{
		TxnId:         txnID,
		Config:        config,
		TxnOperations: []shardkv.TxnOperation{{Key: keyA, Value: "held-a", Op: kvraft.PutOp}},
	}); reply.Err != shardkv.OK {
		t.Fatalf("prepare before crash/reconfig failed on gid 100: %v", reply.Err)
	}
	if reply := cfg.callGroupPrepare(t, 101, &shardkv.PrepareArgs{
		TxnId:         txnID,
		Config:        config,
		TxnOperations: []shardkv.TxnOperation{{Key: keyB, Value: "held-b", Op: kvraft.PutOp}},
	}); reply.Err != shardkv.OK {
		t.Fatalf("prepare before crash/reconfig failed on gid 101: %v", reply.Err)
	}

	fillA := cfg.claimKeyInGID(100)
	fillB := cfg.claimKeyInGID(101)
	for i := 0; i < 100; i++ {
		suffix := strconv.Itoa(i)
		cfg.runTxn(t, cfg.coordClerk, "txn-snapshot-"+suffix, []TxnOperation{
			{Key: fillA, Value: "a-" + suffix, Op: kvraft.PutOp},
			{Key: fillB, Value: "b-" + suffix, Op: kvraft.PutOp},
		}, []string{"a-" + suffix, "b-" + suffix})
	}
	if eventCount(EventSnapshotSave) == 0 {
		t.Fatal("expected coordinator snapshot")
	}
	hasCoordSnapshot := false
	for _, ps := range cfg.coordSaved {
		if ps != nil && ps.SnapshotSize() > 0 {
			hasCoordSnapshot = true
			break
		}
	}
	if !hasCoordSnapshot {
		t.Fatal("expected persisted coordinator snapshot")
	}
	hasShardSnapshot := false
	for _, g := range cfg.groups {
		for _, ps := range g.saved {
			if ps != nil && ps.SnapshotSize() > 0 {
				hasShardSnapshot = true
				break
			}
		}
	}
	if !hasShardSnapshot {
		t.Fatal("expected persisted shard snapshot")
	}
	cfg.smClerk.Leave([]int{100})
	if moved := cfg.smClerk.Query(-1).Shards[shardkv.Key2shard(keyA)]; moved != 101 {
		t.Fatalf("shardctrler did not move shard to gid 101, got %d", moved)
	}

	probe := cfg.makeShardKVClerk()
	defer probe.Kill()
	done := make(chan struct{}, 1)
	go func() {
		probe.Put(keyA, "after-crash-reconfig")
		done <- struct{}{}
	}()
	select {
	case <-done:
		t.Fatal("reconfig should stay blocked while prepared txn is still holding the shard")
	case <-time.After(1 * time.Second):
	}

	cfg.shutdownGroup(0)
	time.Sleep(300 * time.Millisecond)
	cfg.startGroup(0)
	select {
	case <-done:
		t.Fatal("reconfig should stay blocked after group restart until commit")
	case <-time.After(1 * time.Second):
	}

	if got := cfg.coordClerk.TransactionWithConfig(txnID, ops, config); !reflect.DeepEqual(got, []string{"held-a", "held-b"}) {
		t.Fatalf("coordinator retry after restart failed: %v", got)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reconfig did not resume after commit released the prepared txn")
	}
	if got := probe.Get(keyA); got != "after-crash-reconfig" {
		t.Fatalf("probe mismatch after crash/reconfig recovery: got %q", got)
	}
	if got := cfg.kvClerk.Get(keyB); got != "held-b" {
		t.Fatalf("peer group write mismatch after crash/reconfig recovery: got %q", got)
	}
}

func TestTxnPartialPreparedAbort(t *testing.T) {
	cfg := make_config(3, 100, 101)
	defer cfg.cleanup()

	keyA := cfg.claimKeyInGID(100)
	keyB := cfg.claimKeyInGID(101)
	probeB := cfg.claimKeyInGID(101)
	cfg.kvClerk.Put(keyA, "base-a")
	cfg.kvClerk.Put(keyB, "base-b")
	cfg.kvClerk.Put(probeB, "base-probe-b")
	config := cfg.smClerk.Query(-1)
	txnID := "txn-partial-abort"
	if reply := cfg.callGroupPrepare(t, 100, &shardkv.PrepareArgs{
		TxnId:         txnID,
		Config:        config,
		TxnOperations: []shardkv.TxnOperation{{Key: keyA, Value: "prepared-a", Op: kvraft.PutOp}},
	}); reply.Err != shardkv.OK {
		t.Fatalf("prepared side failed: %v", reply.Err)
	}
	if reply := cfg.callGroupAbort(t, 101, &shardkv.AbortArgs{TxnId: txnID}); reply.Err != shardkv.OK {
		t.Fatalf("unprepared side tombstone failed: %v", reply.Err)
	}
	if got := cfg.coordClerk.Transaction(txnID, []TxnOperation{
		{Key: keyA, Value: "prepared-a", Op: kvraft.PutOp},
		{Key: keyB, Value: "prepared-b", Op: kvraft.PutOp},
	}); got != nil {
		t.Fatalf("partial prepared txn should abort globally, got %v", got)
	}
	if got := cfg.kvClerk.Get(keyA); got != "base-a" {
		t.Fatalf("prepared side should roll back after global abort, got %q", got)
	}
	if got := cfg.kvClerk.Get(keyB); got != "base-b" {
		t.Fatalf("unprepared side should stay unchanged after global abort, got %q", got)
	}
	for _, tc := range []struct {
		gid int
		key string
	}{
		{gid: 101, key: keyB},
		{gid: 100, key: keyA},
	} {
		if reply := cfg.callGroupPrepare(t, tc.gid, &shardkv.PrepareArgs{
			TxnId:         txnID,
			Config:        config,
			TxnOperations: []shardkv.TxnOperation{{Key: tc.key, Value: "late", Op: kvraft.PutOp}},
		}); reply.Err != shardkv.ErrTxnAborted {
			t.Fatalf("gid %d got %v", tc.gid, reply.Err)
		}
	}
	cfg.runTxn(t, cfg.coordClerk, "txn-after-partial-abort", []TxnOperation{
		{Key: keyA, Value: "after-a", Op: kvraft.PutOp},
		{Key: probeB, Value: "after-b", Op: kvraft.PutOp},
	}, []string{"after-a", "after-b"})
	if got := cfg.kvClerk.Get(keyA); got != "after-a" {
		t.Fatalf("prepared side should be unlocked after global abort, got %q", got)
	}
	if got := cfg.kvClerk.Get(probeB); got != "after-b" {
		t.Fatalf("new txn on unaffected key should succeed after global abort, got %q", got)
	}
}

func TestTxnConflictMultiThreads(t *testing.T) {
	cfg := make_config(3, 100, 101, 102)
	defer cfg.cleanup()

	type txnResult struct {
		name string
		got  []string
	}
	gids := []int{100, 101, 102}
	conflictKey := cfg.claimKeyInGID(gids[0])
	fillKeys := func() []string {
		keys := make([]string, 9)
		for i := range keys {
			keys[i] = cfg.claimKeyInGID(gids[i%len(gids)])
		}
		return keys
	}
	keysA, keysB, keysC := fillKeys(), fillKeys(), fillKeys()
	startA, startB, startC := make(chan int), make(chan int), make(chan int)
	results := make(chan txnResult, 3)
	worker := func(name string, start <-chan int, ops func(string) []TxnOperation) {
		for round := range start {
			suffix := strconv.Itoa(round)
			got := cfg.coordClerk.Transaction(name+"-"+suffix, ops(suffix))
			results <- txnResult{name: name, got: got}
		}
	}
	buildOps := func(sharedPrefix string, sharedKey string, keys []string) func(string) []TxnOperation {
		return func(suffix string) []TxnOperation {
			ops := make([]TxnOperation, 0, len(keys)+1)
			ops = append(ops, TxnOperation{Key: sharedKey, Value: sharedPrefix + "-shared-" + suffix, Op: kvraft.PutOp})
			for i, key := range keys {
				ops = append(ops, TxnOperation{Key: key, Value: sharedPrefix + "-" + strconv.Itoa(i) + "-" + suffix, Op: kvraft.PutOp})
			}
			return ops
		}
	}
	go worker("conflict-a", startA, buildOps("va", conflictKey, keysA))
	go worker("conflict-b", startB, buildOps("vb", conflictKey, keysB))
	go worker("independent", startC, func(suffix string) []TxnOperation {
		ops := make([]TxnOperation, 0, len(keysC))
		for i, key := range keysC {
			ops = append(ops, TxnOperation{Key: key, Value: "vc-" + strconv.Itoa(i) + "-" + suffix, Op: kvraft.PutOp})
		}
		return ops
	})
	defer close(startA)
	defer close(startB)
	defer close(startC)

	failA, failB := 0, 0
	for round := 0; round < 30; round++ {
		cfg.kvClerk.Put(conflictKey, "base-conflict")
		for i, key := range keysA {
			cfg.kvClerk.Put(key, "base-a-"+strconv.Itoa(i))
		}
		for i, key := range keysB {
			cfg.kvClerk.Put(key, "base-b-"+strconv.Itoa(i))
		}
		for i, key := range keysC {
			cfg.kvClerk.Put(key, "base-c-"+strconv.Itoa(i))
		}
		startA <- round
		startB <- round
		startC <- round

		suffix := strconv.Itoa(round)
		successA, successB, successC := false, false, false
		for i := 0; i < 3; i++ {
			result := <-results
			switch result.name {
			case "conflict-a":
				successA = result.got != nil
				if !successA {
					failA++
				} else if len(result.got) != len(keysA)+1 {
					t.Fatalf("unexpected result for conflict-a: %v", result.got)
				}
			case "conflict-b":
				successB = result.got != nil
				if !successB {
					failB++
				} else if len(result.got) != len(keysB)+1 {
					t.Fatalf("unexpected result for conflict-b: %v", result.got)
				}
			case "independent":
				successC = result.got != nil
				if !successC || len(result.got) != len(keysC) {
					t.Fatalf("unexpected result for independent txn: %v", result.got)
				}
			}
		}
		if !successC {
			t.Fatalf("independent txn should always succeed")
		}
		if successA && successB {
			if got := cfg.kvClerk.Get(conflictKey); got != "va-shared-"+suffix && got != "vb-shared-"+suffix {
				t.Fatalf("conflictKey mismatch after dual success: got %q", got)
			}
		} else if successA {
			if got := cfg.kvClerk.Get(conflictKey); got != "va-shared-"+suffix {
				t.Fatalf("conflictKey mismatch: got %q want %q", got, "va-shared-"+suffix)
			}
		} else if successB {
			if got := cfg.kvClerk.Get(conflictKey); got != "vb-shared-"+suffix {
				t.Fatalf("conflictKey mismatch: got %q want %q", got, "vb-shared-"+suffix)
			}
		}
		for i, key := range keysC {
			want := "vc-" + strconv.Itoa(i) + "-" + suffix
			if got := cfg.kvClerk.Get(key); got != want {
				t.Fatalf("independent key %q mismatch: got %q want %q", key, got, want)
			}
		}
	}
	t.Logf("conflict failures: A=%d B=%d", failA, failB)
	if failA == 0 || failB == 0 {
		t.Fatalf("expected both conflicting workers to fail at least once, got A=%d B=%d", failA, failB)
	}
}

func TestTxnTriangleConflict(t *testing.T) {
	cfg := make_config(3, 100)
	defer cfg.cleanup()

	leftKey := cfg.claimKeyInGID(100)
	rightKey := cfg.claimKeyInGID(100)
	reset := func(left, right string) {
		cfg.kvClerk.Put(leftKey, left)
		cfg.kvClerk.Put(rightKey, right)
	}
	check := func(round int, key, got, want string) {
		if got != want {
			t.Fatalf("round %d: %s mismatch: got %q want %q", round, key, got, want)
		}
	}
	prepare := func(round int, txnID string, ops []shardkv.TxnOperation, config shardctrler.Config) {
		if reply := cfg.callGroupPrepare(t, 100, &shardkv.PrepareArgs{
			TxnId:         txnID,
			Config:        config,
			TxnOperations: ops,
		}); reply.Err != shardkv.OK {
			t.Fatalf("round %d: prepare %s failed: %v", round, txnID, reply.Err)
		}
	}
	commit := func(round int, txnID string, want []string) {
		if got := cfg.callGroupCommit(t, 100, &shardkv.CommitArgs{TxnId: txnID}); got.Err != shardkv.OK || !reflect.DeepEqual(got.Values, want) {
			t.Fatalf("round %d: commit %s failed: %+v", round, txnID, got)
		}
	}
	for round := 0; round < 20; round++ {
		suffix := strconv.Itoa(round)
		config := cfg.smClerk.Query(-1)
		middleHolder := "txn-middle-holder-" + suffix
		leftHolder := "txn-left-holder-" + suffix
		rightHolder := "txn-right-holder-" + suffix
		leftWin := "left-win-" + suffix
		rightWin := "right-win-" + suffix
		middleLeft := "middle-left-" + suffix
		middleRight := "middle-right-" + suffix

		reset("left-base-"+suffix, "right-base-"+suffix)
		prepare(round, middleHolder, []shardkv.TxnOperation{
			{Key: leftKey, Value: middleLeft, Op: kvraft.PutOp},
			{Key: rightKey, Value: middleRight, Op: kvraft.PutOp},
		}, config)
		if got := cfg.coordClerk.Transaction("txn-left-lose-"+suffix, []TxnOperation{
			{Key: leftKey, Value: leftWin, Op: kvraft.PutOp},
		}); got != nil {
			t.Fatalf("round %d: left txn should fail while middle holds both keys, got %v", round, got)
		}
		if got := cfg.coordClerk.Transaction("txn-right-lose-"+suffix, []TxnOperation{
			{Key: rightKey, Value: rightWin, Op: kvraft.PutOp},
		}); got != nil {
			t.Fatalf("round %d: right txn should fail while middle holds both keys, got %v", round, got)
		}
		if reply := cfg.callGroupAbort(t, 100, &shardkv.AbortArgs{TxnId: middleHolder}); reply.Err != shardkv.OK {
			t.Fatalf("round %d: abort middle holder failed: %v", round, reply.Err)
		}
		check(round, leftKey, cfg.kvClerk.Get(leftKey), "left-base-"+suffix)
		check(round, rightKey, cfg.kvClerk.Get(rightKey), "right-base-"+suffix)

		reset("left-base-2-"+suffix, "right-base-2-"+suffix)
		prepare(round, leftHolder, []shardkv.TxnOperation{{Key: leftKey, Value: leftWin, Op: kvraft.PutOp}}, config)
		prepare(round, rightHolder, []shardkv.TxnOperation{{Key: rightKey, Value: rightWin, Op: kvraft.PutOp}}, config)
		if got := cfg.coordClerk.Transaction("txn-middle-lose-"+suffix, []TxnOperation{
			{Key: leftKey, Value: middleLeft, Op: kvraft.PutOp},
			{Key: rightKey, Value: middleRight, Op: kvraft.PutOp},
		}); got != nil {
			t.Fatalf("round %d: middle txn should fail while both side txns hold locks, got %v", round, got)
		}
		commit(round, leftHolder, []string{leftWin})
		commit(round, rightHolder, []string{rightWin})
		check(round, leftKey, cfg.kvClerk.Get(leftKey), leftWin)
		check(round, rightKey, cfg.kvClerk.Get(rightKey), rightWin)
	}
}

type randomTxnOptions struct {
	unreliable    bool
	crash         bool
	reconfig      bool
	recordHistory bool
	rounds        int
	workers       int
	opsPerTxn     int
}

func runRandomTxnTest(t *testing.T, opts randomTxnOptions) []porcupine.Operation {
	resetEvents()
	gids := []int{100, 101, 102, 103, 104}
	cfg := make_config(3, gids...)
	defer cfg.cleanup()
	cfg.net.Reliable(!opts.unreliable)
	var opLog txnOpLog
	t0 := time.Now()

	workerKeys := make([][]string, opts.workers)
	expected := make([][]string, opts.workers)
	var checkMu sync.RWMutex
	for w := 0; w < opts.workers; w++ {
		workerKeys[w] = make([]string, 0, len(gids))
		for _, gid := range gids {
			workerKeys[w] = append(workerKeys[w], cfg.claimKeyInGID(gid))
		}
		expected[w] = make([]string, len(workerKeys[w]))
		for _, key := range workerKeys[w] {
			cfg.kvClerk.Put(key, "")
		}
	}
	checkTxnResult := func() {
		checkMu.Lock()
		defer checkMu.Unlock()
		for w := range workerKeys {
			for i, key := range workerKeys[w] {
				if got := cfg.kvClerk.Get(key); got != expected[w][i] {
					t.Fatalf("worker %d key %s mismatch: got %q want %q", w, key, got, expected[w][i])
				}
			}
		}
	}

	var done atomic.Int32
	var successes atomic.Int32
	var aborts atomic.Int32
	finishCh := make(chan bool, opts.workers)
	for worker := 0; worker < opts.workers; worker++ {
		go func(worker int) {
			defer func() { finishCh <- true }()
			ck := cfg.makeCoordinatorClerk()
			defer ck.Kill()
			r := rand.New(rand.NewSource(int64(worker + 1)))
			seq := 0
			for done.Load() == 0 {
				checkMu.RLock()
				ops := make([]TxnOperation, 0, opts.opsPerTxn)
				next := append([]string(nil), expected[worker]...)
				appendOp := func(keyIdx, i int) {
					key := workerKeys[worker][keyIdx]
					switch r.Intn(3) {
					case 0:
						ops = append(ops, TxnOperation{Key: key, Op: kvraft.GetOp})
					case 1:
						value := "put-" + strconv.Itoa(worker) + "-" + strconv.Itoa(seq) + "-" + strconv.Itoa(i)
						ops = append(ops, TxnOperation{Key: key, Value: value, Op: kvraft.PutOp})
						next[keyIdx] = value
					default:
						value := "+app-" + strconv.Itoa(worker) + "-" + strconv.Itoa(seq) + "-" + strconv.Itoa(i)
						ops = append(ops, TxnOperation{Key: key, Value: value, Op: kvraft.AppendOp})
						next[keyIdx] += value
					}
				}
				switch r.Intn(3) {
				case 0:
					keyIdx := r.Intn(len(workerKeys[worker]))
					for i := 0; i < opts.opsPerTxn; i++ {
						appendOp(keyIdx, i)
					}
				case 1:
					keyIdx1 := r.Intn(len(workerKeys[worker]))
					keyIdx2 := r.Intn(len(workerKeys[worker]))
					for keyIdx2 == keyIdx1 {
						keyIdx2 = (keyIdx2 + 1) % len(workerKeys[worker])
					}
					for i := 0; i < opts.opsPerTxn; i++ {
						if i%2 == 0 {
							appendOp(keyIdx1, i)
						} else {
							appendOp(keyIdx2, i)
						}
					}
				default:
					for i := 0; i < opts.opsPerTxn; i++ {
						appendOp(r.Intn(len(workerKeys[worker])), i)
					}
				}
				start := int64(time.Since(t0))
				txnId := "txn-random-" + strconv.Itoa(worker) + "-" + strconv.Itoa(seq)
				values := ck.Transaction(txnId, ops)
				end := int64(time.Since(t0))
				if values != nil {
					copy(expected[worker], next)
					successes.Add(1)
				} else {
					aborts.Add(1)
				}
				if opts.recordHistory {
					out := TxnModelOutput{Err: shardkv.ErrTxnAborted}
					if values != nil {
						out.Err = shardkv.OK
						out.Values = values
					}
					opLog.Append(porcupine.Operation{
						ClientId: worker,
						Input: TxnModelInput{
							TxnId: txnId,
							Ops:   ops,
						},
						Output: out,
						Call:   start,
						Return: end,
					})
				}
				checkMu.RUnlock()
				if seq%10 == 9 {
					checkTxnResult()
				}
				seq++
			}
		}(worker)
	}

	r := rand.New(rand.NewSource(99))
	joined := map[int]bool{}
	groupServers := map[int][]string{}
	joinedCount := 0
	for _, g := range cfg.groups {
		joined[g.gid] = true
		groupServers[g.gid] = append([]string(nil), g.names...)
		joinedCount++
	}
	restarts, reconfigs := 0, 0
	for round := 0; round < opts.rounds; round++ {
		if round > opts.rounds/2 {
			cfg.net.LongReordering(opts.unreliable)
		}
		if opts.reconfig {
			gid := gids[r.Intn(len(gids))]
			if joined[gid] && joinedCount > 1 {
				cfg.smClerk.Leave([]int{gid})
				joined[gid] = false
				joinedCount--
			} else if !joined[gid] {
				cfg.smClerk.Join(map[int][]string{gid: groupServers[gid]})
				joined[gid] = true
				joinedCount++
			}
			reconfigs++
		}
		if opts.crash {
			i := r.Intn(cfg.nservers)
			cfg.shutdownCoordinator(i)
			time.Sleep(time.Duration(50+r.Intn(100)) * time.Millisecond)
			cfg.startCoordinator(i)
			restarts++
		}
		time.Sleep(time.Duration(100+r.Intn(150)) * time.Millisecond)
	}

	done.Store(1)
	if opts.unreliable {
		cfg.net.Reliable(true)
		cfg.net.LongReordering(false)
	}
	for worker := 0; worker < opts.workers; worker++ {
		<-finishCh
	}
	checkTxnResult()
	t.Logf("random txn commits=%d aborts=%d restarts=%d reconfigs=%d unreliable=%v crash=%v reconfig=%v snapshot=%d",
		successes.Load(), aborts.Load(), restarts, reconfigs, opts.unreliable, opts.crash, opts.reconfig, eventCount(EventSnapshotSave))
	t.Logf("random txn events: prepare_wrong_group=%d prepare_failed=%d commit_retry=%d abort_retry=%d recovery_prepare=%d recovery_commit=%d recovery_abort=%d",
		eventCount(EventWrongGroup, TxnStatusPrepare),
		eventCount(EventPrepareFailed),
		eventCount(EventFinalActionRetry, TxnStatusCommit),
		eventCount(EventFinalActionRetry, TxnStatusAbort),
		eventCount(EventRecoveryDrivePrepare),
		eventCount(EventRecoveryDriveCommit),
		eventCount(EventRecoveryDriveAbort))
	if opts.recordHistory {
		return opLog.Read()
	}
	return nil
}

func TestRandomTxnStable(t *testing.T) {
	runRandomTxnTest(t, randomTxnOptions{rounds: 20, workers: 3, opsPerTxn: 5})
}

func TestRandomTxnUnreliable(t *testing.T) {
	runRandomTxnTest(t, randomTxnOptions{unreliable: true, rounds: 50, workers: 5, opsPerTxn: 5})
}

func TestRandomTxnCrash(t *testing.T) {
	runRandomTxnTest(t, randomTxnOptions{crash: true, rounds: 50, workers: 5, opsPerTxn: 5})
}

func TestRandomTxnReconfig(t *testing.T) {
	runRandomTxnTest(t, randomTxnOptions{reconfig: true, rounds: 50, workers: 5, opsPerTxn: 6})
}

func TestRandomTxnChaos(t *testing.T) {
	runRandomTxnTest(t, randomTxnOptions{
		unreliable: true,
		crash:      true,
		reconfig:   true,
		rounds:     50,
		workers:    5,
		opsPerTxn:  6,
	})
	rpcFailureHits := eventCount(EventPrepareFailed) +
		eventCount(EventFinalActionRetry, TxnStatusCommit) +
		eventCount(EventFinalActionRetry, TxnStatusAbort)
	if rpcFailureHits == 0 {
		t.Fatalf("expected prepare/commit/abort disturbance path")
	}
	if eventCount(EventSnapshotSave) == 0 || eventCount(EventSnapshotLoad) == 0 {
		t.Fatalf("expected snapshot save/load, got save=%d load=%d", eventCount(EventSnapshotSave), eventCount(EventSnapshotLoad))
	}
	recoveryHits := eventCount(EventRecoveryDrivePrepare) + eventCount(EventRecoveryDriveCommit) + eventCount(EventRecoveryDriveAbort)
	if recoveryHits == 0 {
		t.Fatalf("expected recovery driver activity")
	}
}

func TestRandomTxnLinearizable(t *testing.T) {
	operations := runRandomTxnTest(t, randomTxnOptions{
		recordHistory: true,
		unreliable:    true,
		crash:         true,
		reconfig:      true,
		rounds:        50,
		workers:       3,
		opsPerTxn:     6,
	})
	res, info := porcupine.CheckOperationsVerbose(TxnModel, operations, linearizabilityCheckTimeout)
	writeToFile := func() {
		file, err := ioutil.TempFile("", "*.html")
		if err != nil {
			fmt.Printf("info: failed to create temp file for visualization")
		} else {
			err = porcupine.Visualize(TxnModel, info, file)
			if err != nil {
				fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
			} else {
				fmt.Printf("info: wrote history visualization to %s\n", file.Name())
			}
		}
	}
	switch res {
	case porcupine.Illegal:
		writeToFile()
		t.Fatal("history is not linearizable")
	case porcupine.Unknown:
		t.Logf("info: linearizability check timed out, assuming history is ok")
	default:
		// writeToFile()
	}
}

func TestParticipantAbortBeforePrepare(t *testing.T) {
	cfg := make_config(3, 100, 101)
	defer cfg.cleanup()

	key := cfg.claimKeyInGID(100)
	otherKey := cfg.claimKeyInGID(101)
	config := cfg.smClerk.Query(-1)
	if reply := cfg.callGroupAbort(t, 100, &shardkv.AbortArgs{TxnId: "txn-tombstone"}); reply.Err != shardkv.OK {
		t.Fatalf("abort tombstone failed: %v", reply.Err)
	}
	reply := cfg.callGroupPrepare(t, 100, &shardkv.PrepareArgs{
		TxnId:         "txn-tombstone",
		Config:        config,
		TxnOperations: []shardkv.TxnOperation{{Key: key, Value: "x", Op: kvraft.PutOp}},
	})
	if reply.Err != shardkv.ErrTxnAborted {
		t.Fatalf("prepare after abort should be rejected, got %v", reply.Err)
	}
	if got := cfg.coordClerk.Transaction("txn-tombstone", []TxnOperation{
		{Key: key, Value: "x", Op: kvraft.PutOp},
		{Key: otherKey, Value: "y", Op: kvraft.PutOp},
	}); got != nil {
		t.Fatalf("txn with participant tombstone should fail, got %v", got)
	}
}

func TestParticipantCommitIdempotent(t *testing.T) {
	cfg := make_config(3, 100)
	defer cfg.cleanup()

	key := cfg.claimKeyInGID(100)
	cfg.kvClerk.Put(key, "base")
	config := cfg.smClerk.Query(-1)
	if reply := cfg.callGroupPrepare(t, 100, &shardkv.PrepareArgs{
		TxnId:  "txn-commit-idempotent",
		Config: config,
		TxnOperations: []shardkv.TxnOperation{
			{Key: key, Op: kvraft.GetOp},
			{Key: key, Value: "+x", Op: kvraft.AppendOp},
		},
	}); reply.Err != shardkv.OK {
		t.Fatalf("prepare failed: %v", reply.Err)
	}
	got1 := cfg.callGroupCommit(t, 100, &shardkv.CommitArgs{TxnId: "txn-commit-idempotent"})
	got2 := cfg.callGroupCommit(t, 100, &shardkv.CommitArgs{TxnId: "txn-commit-idempotent"})
	want := []string{"base", "base+x"}
	if got1.Err != shardkv.OK || got2.Err != shardkv.OK || !reflect.DeepEqual(got1.Values, want) || !reflect.DeepEqual(got1.Values, got2.Values) {
		t.Fatalf("unexpected commit results: %+v %+v", got1, got2)
	}
	if got := cfg.kvClerk.Get(key); got != "base+x" {
		t.Fatalf("append not committed: got %q", got)
	}
}
