# ternary-event: Pub/sub event dispatch with ternary priorities

An in-process event bus where every event carries one of three priorities — **Low**, **Normal**, or **Critical**. Subscribers filter by event type, priority, and source. An append-only history log enables replay for late joiners or debugging.

## Why This Exists

In multi-agent systems, not all events are equal. A disk-full alert matters more than a heartbeat tick. Binary priority (high/low) isn't enough — you need a middle ground for normal operational events. Ternary priorities give you Low (informational), Normal (operational), and Critical (must-handle-now) without the overhead of arbitrary priority levels.

## Core Concepts

- **Priority**: A ternary enum — `Low`, `Normal`, `Critical`. Ordered: `Low < Normal < Critical`.
- **Event**: A typed payload with priority, source, and timestamp. The core unit of data flowing through the system.
- **EventBus**: Central dispatcher. Publishers send events; subscribers with matching filters receive them synchronously.
- **EventFilter**: Pattern matcher on event streams. Filter by type, minimum priority, and/or source. All conditions AND together.
- **Subscription**: A registered listener with a filter and callback function pointer.
- **EventHistory**: Append-only log with optional capacity cap. Supports full replay and filtered replay.
- **EventEmitter**: A convenience trait for types that can produce events.

## Quick Start

```toml
# Cargo.toml
[dependencies]
ternary-event = "0.1"
```

```rust
use ternary_event::*;

fn main() {
    let mut bus = EventBus::new();

    // Subscribe to critical alerts from the monitor
    let id = bus.subscribe(
        EventFilter::new()
            .event_type("alert")
            .min_priority(Priority::Critical),
        |event| println!("ALERT: {}", event.payload),
    );

    // Publish an event
    bus.publish(Event::new("alert", "disk 95% full", Priority::Critical, "monitor"));

    // Later, unsubscribe
    bus.unsubscribe(id);
}
```

## API Overview

| Type | Description |
|------|-------------|
| `Priority` | Ternary enum: Low, Normal, Critical (ordered) |
| `Event` | Typed payload with priority, source, and timestamp |
| `EventFilter` | Pattern matcher: filter by type, min priority, source |
| `EventBus` | Central pub/sub dispatcher with history |
| `Subscription` | Registered listener (id + filter + callback) |
| `EventHistory` | Append-only log with capacity and replay |
| `EventEmitter` | Trait for types that produce events |

## How It Works

`EventBus` maintains a `HashMap` of subscriptions indexed by ID. When `publish` is called, it iterates all subscriptions, checks each filter against the event, and calls matching callbacks synchronously. The event is then appended to the internal `EventHistory`.

`EventFilter` uses builder-pattern methods (`event_type()`, `min_priority()`, `source()`) that AND together. An empty filter matches everything.

`EventHistory` is a `Vec` with an optional capacity cap. When full, the oldest event is evicted (FIFO). The `replay` method returns an iterator; `replay_filtered` applies an `EventFilter`.

Callbacks are function pointers (`fn(&Event)`), not closures, to keep the design simple and `Clone`-free.

## Known Limitations

- **Synchronous dispatch only**: All callbacks run on the calling thread. Long-running callbacks will block the publisher. No async support.
- **Function pointers, not closures**: You can't capture state in callbacks. Use `static` atomics or external state for accumulation.
- **No error handling in callbacks**: A panicking callback will propagate the panic through `publish`. There's no catch-and-continue mechanism.
- **Linear fan-out**: Every publish iterates all subscriptions. O(n) in subscription count. Fine for hundreds; not ideal for millions.
- **History eviction is O(n)**: `Vec::remove(0)` shifts all elements. Acceptable for small-to-medium histories.
- **No wildcard event types**: Filters match exact strings. No glob or regex support.

## Use Cases

- **Agent coordination**: Agents publish task-completed events; supervisors subscribe to track progress.
- **System monitoring**: Sensors emit events with Critical priority for threshold breaches; alerting systems subscribe accordingly.
- **Game engine events**: Entity actions published to a bus; UI, AI, and scoring systems each subscribe with different filters.
- **Audit logging**: All events are recorded in history. Compliance systems replay the log to reconstruct timelines.

## Ecosystem Context

Part of the **SuperInstance** ternary crate family. Relates to:

- **ternary-command**: Command execution can publish events (e.g., "command failed")
- **ternary-trust**: Trust events (betrayal, cooperation) can flow through the event bus
- **ternary-inventory**: Inventory changes published as events for dependent systems

This crate is a leaf dependency — it doesn't depend on other ternary crates.

## License

MIT

## See Also
- **ternary-bus** — related
- **ternary-channel** — related
- **ternary-room** — related
- **ternary-chronicle** — related
- **ternary-replay** — related

