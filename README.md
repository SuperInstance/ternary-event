# ternary-event

**Pub/sub event dispatch with ternary priorities for the SuperInstance ecosystem.**

## Background

Event-driven architecture is the backbone of reactive systems — from browser DOM events to Kubernetes watch streams to Apache Kafka topics. The key challenge is **filtering**: subscribers want specific events without drowning in noise. Traditional systems use topic-based routing or content-based filtering, but few embed priority semantics directly into the event model.

`ternary-event` provides an in-process event bus where every event carries a **ternary priority** — `Low`, `Normal`, or `Critical` — that subscribers can filter on. Combined with type-based and source-based filtering, this enables fine-grained subscription policies. An append-only history log supports replay for late joiners, debugging, and audit.

## How It Works

### Event Model

Every event has five fields:

| Field | Type | Description |
|-------|------|-------------|
| `event_type` | String | Semantic category (e.g., "room.joined", "task.failed") |
| `payload` | String | Event data |
| `priority` | Priority | `Low`, `Normal`, or `Critical` |
| `timestamp_ms` | u64 | Unix epoch milliseconds |
| `source` | String | Originating agent or room |

### Subscription and Filtering

`EventFilter` supports three independent filter axes (combined with AND logic):

- **`event_type`** — exact match on event category
- **`min_priority`** — minimum priority threshold (e.g., `Normal` matches `Normal` and `Critical`)
- **`source`** — exact match on event origin

Subscribers register filters with callbacks: `bus.subscribe(filter, |event| { ... })`. The bus invokes matching callbacks synchronously on `publish()`.

### Event History

`EventHistory` is a bounded append-only log. When at capacity, the oldest event is evicted. It supports:

- **Full replay** — `replay()` iterates all stored events
- **Filtered replay** — `replay_filtered(&filter)` returns only matching events
- **Late joiners** — `bus.replay_to(callback)` replays all history to a new subscriber

### Priority Ordering

`Priority` implements `Ord`: `Low < Normal < Critical`. This enables priority-based filtering (only critical alerts), priority queues (process critical events first), and priority-aware routing in higher-level systems.

## Experimental Results

The test suite (25+ tests) validates:

- **Priority ordering** — `Low < Normal < Critical` holds for all pairs
- **Filter matching** — type-only, priority-only, source-only, and combined filters correctly match/reject events
- **Empty filter** — matches everything (no filtering)
- **History lifecycle** — append, capacity-based eviction, last-event query, filtered replay
- **Bus dispatch** — matching subscribers receive events; non-matching subscribers are skipped
- **History integration** — published events are stored in bus history
- **Replay** — `replay_to()` delivers all history events to a callback

## Impact

The ternary priority model addresses a real gap in event systems. In fleet management, not all events are equal: a node going offline (`Critical`) shouldn't wait behind routine telemetry (`Low`). By baking priority into the event model, `ternary-event` ensures that priority-aware processing is the default, not an afterthought.

The history/replay capability enables event-sourcing patterns: new subscribers can reconstruct state from the event log, and debugging becomes a matter of replaying filtered history rather than searching logs.

## Use Cases

1. **Fleet health monitoring** — Rooms publish health events with priorities: `Low` for routine heartbeats, `Normal` for resource warnings, `Critical` for node failures. A monitoring dashboard subscribes with `min_priority(Normal)` to filter out noise.

2. **Debugging and forensics** — When a fleet incident occurs, `history.replay_filtered(&EventFilter::new().source("room-7"))` surfaces every event from the affected room, with timestamps for timeline reconstruction.

3. **Late-joining agents** — A new agent connects to the fleet and calls `bus.replay_to(callback)` to catch up on all events it missed, enabling state reconstruction without explicit synchronization.

4. **Alert escalation** — A subscriber registers for `min_priority(Critical)` events and triggers escalation workflows (email, Slack, auto-remediation) only for the most urgent events, while another subscriber handles all events for metrics aggregation.

5. **Event sourcing** — Room state is derived from the event history: replay the full event log to reconstruct current state, enabling time-travel debugging and state snapshots.

## Open Questions

- **Async callbacks:** The current callback type is `fn(&Event)` — a synchronous function pointer. Should the bus support `async` callbacks or `Box<dyn Fn>` for closures that capture state?
- **Event ordering:** Events within the same priority are processed in publication order, but there's no cross-priority ordering guarantee. Should `Critical` events always be processed before `Normal` events, even if published later?
- **Persistence:** History is in-memory only. For production deployments, should the history log be persistable (Kafka-like commit log, SQLite-backed, or shared memory)?

## Connection to Oxide Stack

`ternary-event` is the observability layer of the SuperInstance ecosystem:

- **`ternary-channel`** — events may be transported over channels for cross-node delivery
- **`ternary-command`** — command execution publishes events (command.started, command.completed) for observability
- **`ternary-protocol`** — event serialization for wire transport
- **`ternary-fire`** — cellular automaton state changes emit events for visualization
- **`ternary-voting`** — consensus rounds emit events for vote tracking

The priority model (`Low`/`Normal`/`Critical`) maps to the ternary values used throughout the ecosystem, ensuring consistent semantics from the event bus down to the channel layer.
