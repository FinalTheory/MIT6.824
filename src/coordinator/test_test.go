package coordinator

import (
	"reflect"
	"strconv"
	"testing"

	"6.5840/kvraft"
	"6.5840/shardctrler"
	"6.5840/shardkv"
)

func TestBasicTxnSmoke(t *testing.T) {
	cfg := make_config(3, 100, 101)
	defer cfg.cleanup()

	checkKV := func(key, want string) {
		if got := cfg.kvClerk.Get(key); got != want {
			t.Fatalf("%s mismatch after txns: got %q want %q", key, got, want)
		}
	}

	seedKey := cfg.claimKeyInGID(100)
	targetKey := cfg.claimKeyInGID(101)
	finalKey := cfg.claimKeyInGID(100)
	cfg.kvClerk.Put(seedKey, "before")

	cfg.runTxn(t, cfg.coordClerk, "txn-basic-smoke-1", []TxnOperation{
		{Key: seedKey, Op: kvraft.GetOp},
		{Key: targetKey, Value: "value", Op: kvraft.PutOp},
	}, []string{"before", "value"})
	cfg.runTxn(t, cfg.coordClerk, "txn-basic-smoke-2", []TxnOperation{
		{Key: targetKey, Op: kvraft.GetOp},
		{Key: finalKey, Value: "value+after", Op: kvraft.PutOp},
	}, []string{"value", "value+after"})

	checkKV(seedKey, "before")
	checkKV(targetKey, "value")
	checkKV(finalKey, "value+after")
}

func TestTxnIdempotentRetry(t *testing.T) {
	cfg := make_config(3, 100, 101)
	defer cfg.cleanup()

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
