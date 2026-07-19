# QuorumKV

QuorumKV is an in-progress three-node distributed key-value database built in Go. Three independent Node processes use peer gRPC to elect a Leader, replace it after process loss, and expose each Node's locally observed role, Leader, and Term. The deterministic Raft core remains free of networking, disk I/O, and clocks.

## Run a local Cluster

Copy `quorumkv.example.yaml` three times. Keep the shared `cluster_id` and `members` map identical, and set each file's `node.id` and `node.data_dir` to its corresponding Node. Then start all three processes:

```sh
go run ./cmd/quorumkv -config node-1.yaml
go run ./cmd/quorumkv -config node-2.yaml
go run ./cmd/quorumkv -config node-3.yaml
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 status
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 session open
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 set <session-id> 1 greeting hello
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 get greeting
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 delete <session-id> 2 greeting
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 session close <32-hex-character-session-id>
```

The client endpoint implements the versioned `quorumkv.v1.NodeService` status API, explicit Client Session open and close commands, linearizable `GET`, durable sequenced `SET` and `DELETE`, and standard `grpc.health.v1.Health` checks. `DELETE` reports whether a Value existed and succeeds when the Key is already absent. A Follower returns a typed Leader hint, which `quorumkvctl` follows directly within its deadline. Successful reads use a fresh Quorum confirmation and wait for the Leader to apply the captured committed prefix. Session creation is committed through Raft before its random 128-bit identity is returned, and `active_session_limit` bounds replicated active-session state.

Check `quorumkv.v1.Liveness` for local process health and `quorumkv.v1.Readiness` for local RPC readiness. Readiness does not claim that a Cluster quorum is available. Peer handshakes fail closed on protocol, Cluster Identity, or Node Identity mismatches.

`GetStatus` is explicitly a local observation: it reports role, known Leader, Term, last-log, commit, applied, and Snapshot positions. Set `node.metrics_address` to expose a local Prometheus text endpoint at `/metrics`; it reports RPCs, elections, Raft RPCs, proposals, client errors/retries, WAL syncs, commit latency, and Snapshot installation/compaction counters. The endpoint makes no claim about Cluster health and never includes Values or secrets. JSON logs include Node Identity, Term, role, Leader, and log positions.

Each Node automatically creates a Snapshot after retained committed-and-applied WAL entry frames reach `snapshot_threshold_bytes` (64 MiB when omitted). Snapshot file encoding and syncing run from an immutable apply-loop clone, so later commands continue while one Snapshot is in progress. Covered complete WAL segments are removed only after the Snapshot and a recovery checkpoint are durable. Tests and demos can also call `Node.CreateSnapshot` to trigger the same path manually.

The Node stops gracefully when its context is canceled or it receives `SIGINT`/`SIGTERM`.
