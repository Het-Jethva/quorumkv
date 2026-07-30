# QuorumKV

QuorumKV is a three-Node distributed key-value database in Go. Each Node is an
independent process with its own identity, network endpoints, WAL, Snapshot
files, and persistent volume. QuorumKV implements its own deterministic Raft
state machine and exposes typed gRPC APIs plus `quorumkvctl`.

Raft, the write-ahead log, and the Snapshot format are written from scratch.
The project depends on gRPC, Protobuf, and a YAML parser; there is no
consensus library and no embedded storage engine underneath it.

```text
quorumkvctl ──gRPC──> any Node ──Raft peer gRPC──> the other two Nodes
                         │                         │
                         └── segmented WAL         └── independent volume
                             + in-memory state
```

## Guarantees

- Mutations are acknowledged only after durable replication to a Quorum,
  commitment, and application by the Leader.
- Successful `GET` commands are linearizable. Followers redirect Clients
  instead of serving stale reads.
- Retrying the same Client Session and sequence has at-most-once effect.
- A surviving two-Node Quorum elects a new Leader and continues after one Node
  fails.
- WAL recovery, Snapshots, and stale-Follower repair preserve committed state
  across supported crashes and restarts.

## 60-second demo

Requirements: Docker and Docker Compose.

```sh
demo/run.sh
```

The walkthrough starts three independently persisted Nodes, performs
`SET`/`GET`/`DELETE`, kills the Leader, confirms majority progress, restarts the
old Leader, demonstrates minority unavailability, and demonstrates automatic
Snapshot/compaction followed by stale-Follower Snapshot recovery. It cleans up
its volumes when it exits. The demo uses a tiny Snapshot threshold so the
recovery path is visible quickly; normal configurations default to 64 MiB.

For a manual local Cluster, copy `quorumkv.example.yaml` three times, keep the
Cluster Identity and member map identical, and change `node.id` and
`node.data_dir` in each copy. Start each with:

```sh
go run ./cmd/quorumkv -config node-1.yaml
go run ./cmd/quorumkv -config node-2.yaml
go run ./cmd/quorumkv -config node-3.yaml
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 status
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 session open
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 set <session-id> 1 greeting hello
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 get greeting
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 delete <session-id> 2 greeting
```

A Follower returns a typed Leader hint and the client follows it directly.
Successful reads perform a fresh Quorum confirmation. `GetStatus` is a local
observation, not a Cluster-health claim. Liveness and readiness are separate
health services; metrics and JSON logs provide operational evidence.

## Correctness evidence

The repository verifies deterministic Raft transitions, WAL and Snapshot
recovery, seeded fault schedules, and real multi-process election, failover,
partition, restart, repair, and Snapshot scenarios. CI runs formatting, the
full Go suite, race detection, vet, static analysis, Protobuf validation, and
Linux/Windows portable coverage. Durable Snapshot and WAL record decoders are
fuzzed because both parse bytes from disk on the crash-recovery path. The
public project makes no production-readiness claim.

See [docs/architecture.md](docs/architecture.md) for consistency and crash
guarantees, Quorum trade-offs, split-brain wording, linearizable versus
eventual consistency, and the explicit v1 non-goals.

## Measured performance

Every mutation takes the full Raft path and is acknowledged only after
`File.Sync` on a Quorum. There is no unsafe-acknowledgement or benchmark-only
storage mode, and GETs are linearizable API reads, not local lookups.

| Command | Throughput | p50 | p95 | p99 |
| --- | --- | --- | --- | --- |
| `SET` (500 commands) | 932 commands/s | 8.0 ms | 11.0 ms | 18.7 ms |
| `GET` (2,000 commands) | 1,282 commands/s | 6.0 ms | 7.3 ms | 15.0 ms |

Three local processes, 1 KiB Values, eight concurrent workers, on an AMD Ryzen
7 8845HS with an NVMe SSD. These are local development measurements with
hardware and workload metadata attached, not production claims; the raw result
and reproduction steps are in [benchmark/README.md](benchmark/README.md).

## Design decisions

Each of these is recorded as an architecture decision record, with the
alternatives that were rejected and why:

- [ADR-0001](docs/adr/0001-implement-raft-core.md) — implement Raft rather than
  adopt a consensus library.
- [ADR-0002](docs/adr/0002-linearizable-client-reads.md) — ReadIndex quorum
  confirmation instead of clock-based leader leases or stale Follower reads.
- [ADR-0003](docs/adr/0003-deduplicate-mutations-in-replicated-state.md) —
  Client Sessions and sequence numbers give retries at-most-once effects.
- [ADR-0004](docs/adr/0004-deterministic-raft-core.md) — keep the core free of
  networking, storage, timers, and generated types so it can be replayed.
- [ADR-0005](docs/adr/0005-purpose-built-wal-and-snapshots.md) — a purpose-built
  framed, checksummed, segmented WAL rather than an embedded database.
- [ADR-0006](docs/adr/0006-retain-client-session-state-for-the-life-of-the-cluster.md)
  — deduplication state is retained for the life of the Cluster, and why every
  way out costs more than v1 will spend.

### What I would do differently

- **Client Session state is never reclaimed.** Closing a Session frees an
  active-session slot but not its record, because that record is what makes a
  retry return a cached result. Replicated state and every Snapshot therefore
  grow with the total number of Sessions ever opened. Lease-based expiry is the
  right fix and needs a replicated expiry entry rather than any Node's clock;
  ADR-0006 covers the design and why v1 does not have it.
- **The API is narrow.** There is no compare-and-swap, so nothing in the data
  contract requires the consensus underneath it. CAS is the smallest addition
  that would change that.
- **Raft timing is fixed at compile time.** Heartbeat interval, election
  timeout, and the check-quorum window are constants; they belong in
  configuration before this runs anywhere but a LAN.
- **Metrics are counters only.** There are no gauges for term, role, commit
  index, or retained Sessions, and no latency histograms, which is thinner
  than an operator would want.

## Replay a deterministic fault schedule

```sh
go run ./cmd/quorumkvsim -seed 42 -steps 1000 -trace .traces/seed-42.json
```

A failed schedule prints its exact replay command and CI retains traces as
artifacts.

## License

QuorumKV is released under the [MIT License](LICENSE).
