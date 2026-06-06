# PLUG_AND_PLAY — Event

> Pub/sub event dispatch with ternary priorities

## 🚀 Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
ternary-event = { git = "https://github.com/SuperInstance/ternary-event" }
```

Use in your code:

```rust
use ternary_event::EventBus;

let mut bus = EventBus::new();
bus.subscribe("user.login", |e| println!("{:?}", e));
bus.emit("user.login", "alice");
```

## 🔗 Integration

This crate is part of the [SuperInstance ternary fleet](https://github.com/SuperInstance). It uses the canonical `Ternary` type from `ternary-types` for cross-crate compatibility.

## 📄 License

MIT
