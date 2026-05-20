# MIT 6.5840 Distributed Systems Code Companion

This repository contains my implementation notes and code experiments based on
MIT 6.5840's distributed systems labs. The main explanations live in the blog
posts below; this repository is the executable companion code for those articles.

## Articles

- [Distributed Consensus Protocol: Survival Guide](https://blog.finaltheory.me/en/research/raft.html)
- [Understanding Raft Linearizable Read from First Principle](https://blog.finaltheory.me/en/research/linearizability.html)
- [Old-School Programming: From Consensus Algorithm to Sharded Database to Distributed Transaction](https://blog.finaltheory.me/en/research/txn.html)

## What Is In This Repo

- `src/raft`: Raft consensus, snapshots, heartbeat-based leader lease, and
  lease read index support.
- `src/kvraft`: a replicated key/value service on top of Raft.
- `src/shardctrler`: shard configuration controller.
- `src/shardkv`: sharded key/value service, shard migration, transaction
  participants, lease reads, follower reads, and batching.
- `src/coordinator`: transaction coordinator and two-phase commit driver.
- `Design.md`: source-oriented design notes for the transaction and
  linearizable-read implementations.

The code is intentionally close to the lab structure, while `Design.md` records
the implementation boundaries and tradeoffs that are easier to understand with
the source open side by side.

## Running Tests

Run package tests from `src`:

```bash
cd src
go test ./raft
go test ./kvraft
go test ./shardctrler
go test ./shardkv
go test ./coordinator
```

Some focused tests for the linearizable-read work:

```bash
cd src
go test -run 'TestLeaseReadOn(Follower|Leader)$' -count=1 ./shardkv
go test -run '^TestLeaseReadAfterElection$' -count=1 ./shardkv
```

## Reading Guide

Use the blog posts for the high-level reasoning and proof narrative. Use this
repo to inspect the concrete mechanics: where Raft proves a safe replicated log
prefix, where ShardKV waits for local apply visibility, and how the transaction
coordinator and participants split two-phase commit state.
