package coordinator

import (
	"testing"
	"time"
)

func TestBasicTxnSmoke(t *testing.T) {
	cfg := make_config(3, 100)
	defer cfg.cleanup()

	kvClerk := cfg.makeShardKVClerk()
	kvClerk.Put("seed", "before")

	ck := cfg.makeCoordinatorClerk()
	defer func() {
		ck.Kill()
	}()

	done1 := make(chan []string, 1)
	go func() {
		done1 <- ck.Transaction("txn-basic-smoke-1", []TxnOperation{
			{Key: "seed", Op: "Get"},
			{Key: "target", Value: "value", Op: "Put"},
		})
	}()

	var result1 []string
	select {
	case result1 = <-done1:
		t.Logf("basic txn smoke txn1 returned %v", result1)
	case <-time.After(8 * time.Second):
		t.Fatal("basic txn smoke txn1 timed out")
	}

	if len(result1) != 2 {
		t.Fatalf("txn1 returned unexpected length: %v", result1)
	}
	if result1[0] != "before" {
		t.Fatalf("txn1 read wrong seed value: %v", result1)
	}
	if result1[1] != "value" {
		t.Fatalf("txn1 write result mismatch: %v", result1)
	}

	done2 := make(chan []string, 1)
	go func() {
		done2 <- ck.Transaction("txn-basic-smoke-2", []TxnOperation{
			{Key: "target", Op: "Get"},
			{Key: "final", Value: "value+after", Op: "Put"},
		})
	}()

	var result2 []string
	select {
	case result2 = <-done2:
		t.Logf("basic txn smoke txn2 returned %v", result2)
	case <-time.After(8 * time.Second):
		t.Fatal("basic txn smoke txn2 timed out")
	}

	if len(result2) != 2 {
		t.Fatalf("txn2 returned unexpected length: %v", result2)
	}
	if result2[0] != "value" {
		t.Fatalf("txn2 did not observe txn1 result: %v", result2)
	}
	if result2[1] != "value+after" {
		t.Fatalf("txn2 write result mismatch: %v", result2)
	}

	if got := kvClerk.Get("seed"); got != "before" {
		t.Fatalf("seed mismatch after txns: got %q", got)
	}
	if got := kvClerk.Get("target"); got != "value" {
		t.Fatalf("target mismatch after txns: got %q", got)
	}
	if got := kvClerk.Get("final"); got != "value+after" {
		t.Fatalf("final mismatch after txns: got %q", got)
	}
}
