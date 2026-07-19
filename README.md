# QuorumKV

QuorumKV is an in-progress three-node distributed key-value database built in Go. It can boot one observable Node and includes a deterministic event-to-action Raft core that elects a Leader and establishes read readiness by durably replicating, committing, and applying a current-Term no-op without networking, disk I/O, or clocks inside the core.

## Run one Node

```sh
go run ./cmd/quorumkv -config quorumkv.example.yaml
go run ./cmd/quorumkvctl -address 127.0.0.1:7400 status
```

The client endpoint implements the versioned `quorumkv.v1.NodeService` status API and standard `grpc.health.v1.Health` checks. Check `quorumkv.v1.Liveness` for local process health and `quorumkv.v1.Readiness` for local RPC readiness. Readiness does not claim that a Cluster quorum is available.

The Node stops gracefully when its context is canceled or it receives `SIGINT`/`SIGTERM`.
