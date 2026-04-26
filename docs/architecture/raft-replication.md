# Raft Replication Architecture

## Overview

KalamDB uses a multi-Raft topology (OpenRaft 0.9) to replicate metadata, jobs, user data, and shared data across nodes. The Raft layer lives in [backend/crates/kalamdb-raft](backend/crates/kalamdb-raft/src/lib.rs) and is accessed through the `CommandExecutor` abstraction so handlers do not branch on cluster vs standalone mode.

- **Multi-group layout**: 1 unified metadata group, 32 user-data shards, 1 shared-data shard by default. Group identity and sharding helpers live in [backend/crates/kalamdb-sharding/src](backend/crates/kalamdb-sharding/src/lib.rs), and group ID decoding accepts the configured user/shared shard ranges.
- **Command path**: Handler → `CommandExecutor` (`DirectExecutor` in standalone or `RaftExecutor` in cluster) → `RaftManager` → `RaftGroup` → OpenRaft log → State machine → Applier → storage/provider.
- **Replication modes**: `Quorum` (fast, default) or `All` (wait for every member to apply) configured via `ReplicationMode` in [backend/crates/kalamdb-raft/src/manager/config.rs](backend/crates/kalamdb-raft/src/manager/config.rs).
- **Transport**: gRPC service in [backend/crates/kalamdb-raft/src/network/service.rs](backend/crates/kalamdb-raft/src/network/service.rs) handles Raft RPCs and follower→leader proposal forwarding.
- **Storage**: Combined in-memory log + state machine storage in [backend/crates/kalamdb-raft/src/storage/raft_store.rs](backend/crates/kalamdb-raft/src/storage/raft_store.rs); snapshots are used for compaction. Real table writes are persisted by appliers after Raft apply.

## Topology & Sharding

- Group IDs encode role and shard: `Meta`, `DataUserShard(n)`, `DataSharedShard(n)`. Numeric IDs are stable for OpenRaft membership and RPC routing.
- User data routing: `hash(user_id) % user_shards` (default 32). Shared tables currently always use shard 0. Helpers live in `ShardRouter` in [backend/crates/kalamdb-sharding/src/lib.rs](backend/crates/kalamdb-sharding/src/lib.rs).
- Table-level helpers: `ShardRouter::route_table` / `table_shard_id` hash `TableId` when table-scoped routing is needed.

## Command Flow (Cluster Mode)

1. Handlers call the `CommandExecutor` interface. `RaftExecutor` (cluster) serializes Raft commands/responses with FlexBuffers and picks the target group (user shard, shared shard, or metadata group) in [backend/crates/kalamdb-raft/src/executor/raft.rs](backend/crates/kalamdb-raft/src/executor/raft.rs).
2. `RaftManager` proposes to the correct `RaftGroup`:
   - `propose_*` APIs forward to the leader if the local node is a follower via `propose_with_forward`.
   - In `ReplicationMode::All`, leaders wait for every member to apply the committed log before returning.
3. SQL write endpoints inspect the statement target and, on followers, forward directly to the target data-group leader instead of routing every write through the meta leader first. The group is chosen from the table definition and authenticated user shard.
4. `RaftGroup` uses OpenRaft to append/replicate and applies entries through its `KalamRaftStorage`, which drives the state machine. If the caller was a follower, the gRPC service forwards the proposal to the leader and relays the result.
5. State machines deserialize the command, enforce idempotency via `last_applied_index`, and invoke an injected *applier* to persist to real providers (RocksDB/Parquet/etc.). Followers run the same apply path so state converges.

## SQL DML & Commit Ordering

- SQL autocommit DML for user/shared tables is staged through the transaction mutation path and committed through the appropriate data Raft group. Direct local SQL provider writes are no longer the normal HTTP SQL write path in cluster mode.
- State machines derive `_commit_seq` deterministically from `(group_id, log_index)` via `commit_seq_from_log_position`. Appliers receive that value and stamp inserted, updated, deleted, and transaction-batch rows with the replicated commit order.
- `CommitSequenceTracker::observe_committed` advances the local high-watermark after persisted apply, so every node observes the same commit sequence for the same Raft entry.
- `_seq` remains the only client-visible live-query resume cursor. `_commit_seq` is internal state-machine metadata used for deterministic visibility and follower-side snapshot gating.

## Live/WebSocket Followers

- Live subscriptions are node-local: each leader or follower serves its own WebSocket clients after applying replicated data locally.
- Before taking an initial snapshot for a subscription, the live layer calls a local apply barrier. In cluster mode this maps the table to its Raft group and waits until the local state machine has applied every log entry already known on that node.
- Snapshot boundaries are computed from the local materialized `MAX(_seq)` instead of a wall-clock Snowflake upper bound. This avoids creating a boundary that can include leader commits not yet applied on a follower.
- WebSocket batch control carries only `last_seq_id`. Clients reconnect or request the next batch with that single `SeqId`; the backend recomputes snapshot boundaries per connection and keeps commit/snapshot markers out of the wire protocol.
- Buffered notifications are gated by backend-owned `_commit_seq` when present, falling back to `_seq` for legacy tables or stream/system paths.

## Network Layer

- Service surface: `RaftRpc` (vote, append_entries, install_snapshot) and `ClientProposal` (follower→leader proposal forwarding) defined in [backend/crates/kalamdb-raft/src/network/service.rs](backend/crates/kalamdb-raft/src/network/service.rs).
- Client side: `RaftNetworkFactory` caches tonic channels and produces `RaftNetwork` clients per group ([backend/crates/kalamdb-raft/src/network/network.rs](backend/crates/kalamdb-raft/src/network/network.rs)).
- Server startup: `start_rpc_server` binds to the advertised RPC port and serves the Raft gRPC server; invoked by `RaftExecutor::start` before starting groups.
- Forwarding: followers call `ClientProposal` with the group ID and command bytes; the leader applies and returns the response. If leadership changed, the leader hint is returned so callers can retry.

## Raft Manager Lifecycle

Implemented in [backend/crates/kalamdb-raft/src/manager/raft_manager.rs](backend/crates/kalamdb-raft/src/manager/raft_manager.rs).

- **Construction**: Creates 1 meta group + N user shards + M shared shards (defaults 32/1). Each group is a `RaftGroup` wrapping its own storage and network factory.
- **Start**: Registers configured peers with every group, starts the RPC server, then starts all Raft groups with OpenRaft configs (heartbeat/election timeouts from `RaftManagerConfig`).
- **Bootstrap (first node)**: `initialize_cluster` seeds every group with this node as the sole voter, then optionally adds peers as learners and promotes them.
- **Adding nodes**: `add_node` registers a learner in every group, waits for catch-up per group, then promotes to voter.
- **Shutdown**: Attempts best-effort leadership transfer (OpenRaft 0.9 lacks explicit transfer) then marks the manager stopped.
- **Replication controls**: `replication_mode` (Quorum/All) and `replication_timeout` are applied per proposal; `total_nodes` drives `propose_with_all_replicas` when strong replication is requested.

## Raft Groups

Defined in [backend/crates/kalamdb-raft/src/manager/raft_group.rs](backend/crates/kalamdb-raft/src/manager/raft_group.rs).

- Wraps an OpenRaft `Raft` instance plus combined storage and network factory for a single group.
- Key operations: `start`, `initialize`, `add_learner`, `wait_for_learner_catchup`, `promote_learner`, `change_membership`, `propose`, `propose_with_all_replicas`, and `propose_with_forward`.
- Forwarding retries with exponential backoff and returns `NotLeader` errors with leader hints when forwarding fails.
- Leadership transfer is a no-op placeholder (OpenRaft 0.9 lacks direct support).

## Storage & Snapshots

Implemented by `KalamRaftStorage` in [backend/crates/kalamdb-raft/src/storage/raft_store.rs](backend/crates/kalamdb-raft/src/storage/raft_store.rs).

- Combined `RaftStorage` (log + state machine) with in-memory BTreeMap log, vote/commit tracking, and snapshot retention. Snapshot builder captures state machine bytes plus membership metadata; snapshots are served to lagging replicas and purge earlier logs.
- OpenRaft RPC/snapshot serialization continues to use bincode helpers in [backend/crates/kalamdb-raft/src/state_machine/serde_helpers.rs](backend/crates/kalamdb-raft/src/state_machine/serde_helpers.rs).
- State machine apply runs on every node; apply errors are logged and surfaced via response payloads when possible.
- **Current limitation**: log/state storage is in-memory; durability relies on higher layers persisting real data via appliers. Crash restart currently depends on application-level recovery.

## State Machines & Appliers

All state machines live under [backend/crates/kalamdb-raft/src/state_machine](backend/crates/kalamdb-raft/src/state_machine).

- **MetaStateMachine**: namespaces, tables, storages, users, and jobs; caches snapshot data and drives metadata appliers.
- **UserDataStateMachine**: per-user tables; routes persistence through `UserDataApplier` ([user_data.rs](backend/crates/kalamdb-raft/src/state_machine/user_data.rs)).
- **SharedDataStateMachine**: shared tables; uses `SharedDataApplier` ([shared_data.rs](backend/crates/kalamdb-raft/src/state_machine/shared_data.rs)).
- Every state machine tracks `last_applied_index` for idempotency and produces snapshots capturing its cached state; row data persistence is delegated to appliers so followers apply real writes too.
- Applier traits and no-op defaults live in [backend/crates/kalamdb-raft/src/applier](backend/crates/kalamdb-raft/src/applier/mod.rs).

## Command Types

Defined in [backend/crates/kalamdb-raft/src/commands](backend/crates/kalamdb-raft/src/commands/mod.rs) and split per group:
- Meta: namespaces/tables/storage metadata, user accounts, login events, locks, and jobs.
- UserData: per-user DML plus live-query registrations.
- SharedData: shared-table DML.
Responses are serialized FlexBuffers payloads returned after apply.

## Membership & Routing Details

- Peer registry: `RaftManager::register_peer` plumbs peer addresses into every `RaftGroup` so OpenRaft can dial peers via the network factory.
- Leader discovery: `current_leader` is per-group; followers respond to forwarded proposals with leader hints. HTTP SQL write forwarding uses the target data group directly when possible. `RaftExecutor::get_cluster_info` aggregates OpenRaft metrics for UI/diagnostics.
- Shard mapping: user shard via user_id hash; shared shard fixed to 0. Default counts exposed as `DEFAULT_USER_DATA_SHARDS` and `DEFAULT_SHARED_DATA_SHARDS` in [backend/crates/kalamdb-raft/src/manager/config.rs](backend/crates/kalamdb-raft/src/manager/config.rs).

## Operational Notes & Limitations

- Leadership transfer is best-effort only during shutdown; OpenRaft re-elects leaders after a departing leader stops.
- Automatic leader balancing across user shards is not implemented yet. OpenRaft 0.9 does not expose a direct leadership-transfer API in the current wrapper, so balancing requires either a newer OpenRaft surface or coordinated restart/election controls.
- Strong replication (`All`) waits for every configured node to apply; cluster size drives the wait set. Misconfigured peer counts can block proposals.
- WAL/log durability is in-memory; ensure appliers persist real data paths (RocksDB/Parquet) for crash recovery.
- Snapshot size is driven by state machine caches; large snapshots can impact install_snapshot traffic. Purge thresholds (`max_in_snapshot_log_to_keep`, `purge_batch_size`) are set in `RaftGroup::start`.
- Storage still creates per-table or per-shard lower-level storage structures in several providers. Consolidating RocksDB column families into a prefixed-key layout is a storage redesign and is not completed by the Raft routing changes.

## Proposed Reliability & Efficiency Improvements

- **Persist Raft log/state**: Move `KalamRaftStorage` from in-memory BTreeMap to a disk-backed store (RocksDB or a filestore partition) so Raft itself survives process restarts without relying solely on appliers. Retain snapshots for compaction but anchor durability on disk.
- **Snapshot tuning**: Enable streaming or chunked snapshot sends and compress snapshot payloads to reduce memory spikes; lower `max_in_snapshot_log_to_keep` and tune snapshot trigger thresholds per group size to minimize in-memory log growth.
- **Stronger failover hygiene**: Add pre-vote and staggered election timeouts per group to reduce split votes; expose health probes and replication-lag metrics for each group via `RaftManager` so the API tier can drain traffic from lagging nodes.
- **Leader balancing across data shards**: Add shard-leader placement goals and metrics first, then implement a safe transfer or drain mechanism when the OpenRaft API surface supports it. Keep the meta group stable and allow user shards to spread leaders for write throughput.
- **Proposal backpressure**: Add queue depth/lag-based backpressure in `RaftExecutor` before proposing, and surface `NotLeader` + `leader_hint` telemetry to callers to cut retry storms during elections.
- **Membership safety rails**: Validate peer counts against `replication_mode` (e.g., prevent `All` with missing voters), and add a periodic reconciler that re-registers peers into all `RaftGroup` network factories to heal stale channel caches.
- **Job fencing**: For job commands in the meta group, include the acting leader term/ID in claims and reject stale claims on followers to avoid double-execution during leadership flaps.
- **Memory caps for state machines**: Bound live-query maps and recent-op buffers in `UserDataStateMachine` and `SharedDataStateMachine`; evict or summarize metrics past a fixed window to keep snapshot payloads small.
- **Adaptive networking**: Batch and pipeline append_entries per target where possible; reuse tonic channels aggressively (already cached) and add exponential backoff jitter for client-proposal forwarding to reduce thundering herds.
- **Observability hooks**: Export per-group gauges for `last_applied`, `replication_lag`, snapshot send/receive sizes, and propose latency; wire them into `ClusterInfo` so operators can alert on lag before it impacts HA.
- **Column-family consolidation**: Replace per-shard/per-table RocksDB column families with a small fixed set of column families and prefix keys by table/user/shard. This should be done behind storage abstractions so Raft group count and storage layout remain independently tunable.

## How to Run Cluster Mode (today)

1. Build `RaftManagerConfig` from cluster TOML (mapped from `kalamdb_configs::ClusterConfig`).
2. Create `RaftManager` and wire appliers once providers are available.
3. Use `RaftExecutor` as the `CommandExecutor` for handlers.
4. Call `RaftExecutor::start()` to launch the RPC server and start all groups.
5. On the first node only, call `initialize_cluster()` to bootstrap membership; then use `add_node` to join additional peers as learners → voters.
6. Use `replication_mode = "all"` when full synchrony is required; otherwise leave the default quorum mode.

## At-a-Glance Module Map

- Manager & groups: [backend/crates/kalamdb-raft/src/manager](backend/crates/kalamdb-raft/src/manager)
- Network transport: [backend/crates/kalamdb-raft/src/network](backend/crates/kalamdb-raft/src/network)
- Executor abstraction: [backend/crates/kalamdb-raft/src/executor](backend/crates/kalamdb-raft/src/executor)
- State machines & appliers: [backend/crates/kalamdb-raft/src/state_machine](backend/crates/kalamdb-raft/src/state_machine) and [backend/crates/kalamdb-raft/src/applier](backend/crates/kalamdb-raft/src/applier)
- Storage adapter: [backend/crates/kalamdb-raft/src/storage](backend/crates/kalamdb-raft/src/storage)
- Command types: [backend/crates/kalamdb-raft/src/commands](backend/crates/kalamdb-raft/src/commands)
