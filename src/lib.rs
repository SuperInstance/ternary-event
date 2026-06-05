#![forbid(unsafe_code)]

//! ternary-event: Pub/sub event dispatch with ternary priorities.
//!
//! An in-process event bus where every event carries a ternary priority
//! (Low, Normal, Critical). Subscribers filter by type and priority, and
//! an append-only history log enables replay for late joiners or debugging.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Priority & Event
// ---------------------------------------------------------------------------

/// Ternary event priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Priority {
    Low,
    Normal,
    Critical,
}

/// A typed event with a payload and ternary priority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub event_type: String,
    pub payload: String,
    pub priority: Priority,
    pub timestamp_ms: u64,
    pub source: String,
}

impl Event {
    /// Create a new event with the current system time.
    pub fn new(
        event_type: impl Into<String>,
        payload: impl Into<String>,
        priority: Priority,
        source: impl Into<String>,
    ) -> Self {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            event_type: event_type.into(),
            payload: payload.into(),
            priority,
            timestamp_ms,
            source: source.into(),
        }
    }

    /// Create an event with an explicit timestamp (useful in tests).
    pub fn with_timestamp(
        event_type: impl Into<String>,
        payload: impl Into<String>,
        priority: Priority,
        source: impl Into<String>,
        ts: u64,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            payload: payload.into(),
            priority,
            timestamp_ms: ts,
            source: source.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// EventFilter
// ---------------------------------------------------------------------------

/// Pattern-matching filter for event streams.
#[derive(Debug, Clone, Default)]
pub struct EventFilter {
    pub event_type: Option<String>,
    pub min_priority: Option<Priority>,
    pub source: Option<String>,
}

impl EventFilter {
    /// Create an empty filter that matches everything.
    pub fn new() -> Self {
        Self::default()
    }

    /// Only match events of this type.
    pub fn event_type(mut self, t: impl Into<String>) -> Self {
        self.event_type = Some(t.into());
        self
    }

    /// Only match events with at least this priority.
    pub fn min_priority(mut self, p: Priority) -> Self {
        self.min_priority = Some(p);
        self
    }

    /// Only match events from this source.
    pub fn source(mut self, s: impl Into<String>) -> Self {
        self.source = Some(s.into());
        self
    }

    /// Check whether an event matches this filter.
    pub fn matches(&self, event: &Event) -> bool {
        if let Some(ref t) = self.event_type {
            if event.event_type != *t {
                return false;
            }
        }
        if let Some(min) = self.min_priority {
            if event.priority < min {
                return false;
            }
        }
        if let Some(ref s) = self.source {
            if event.source != *s {
                return false;
            }
        }
        true
    }
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

/// A subscriber ID, used to unsubscribe.
pub type SubscriberId = u64;

/// A filtered listener registered on the bus.
#[derive(Debug)]
pub struct Subscription {
    pub id: SubscriberId,
    pub filter: EventFilter,
    pub callback: fn(&Event),
}

// ---------------------------------------------------------------------------
// EventEmitter trait
// ---------------------------------------------------------------------------

/// Convenience trait for types that can emit events.
pub trait EventEmitter {
    fn emit(&self, event_type: &str, payload: &str, priority: Priority) -> Event;
}

/// Blanket impl for any type that has a name (simple demonstration).
impl EventEmitter for str {
    fn emit(&self, event_type: &str, payload: &str, priority: Priority) -> Event {
        Event::new(event_type, payload, priority, self)
    }
}

// ---------------------------------------------------------------------------
// EventHistory
// ---------------------------------------------------------------------------

/// Append-only log of events with replay capability.
#[derive(Debug, Clone)]
pub struct EventHistory {
    events: Vec<Event>,
    max_events: usize,
}

impl EventHistory {
    /// Create an unbounded history.
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            max_events: usize::MAX,
        }
    }

    /// Create a history with a maximum number of retained events.
    pub fn with_capacity(max: usize) -> Self {
        Self {
            events: Vec::new(),
            max_events: max,
        }
    }

    /// Append an event. Evicts oldest if at capacity.
    pub fn append(&mut self, event: Event) {
        if self.events.len() >= self.max_events {
            self.events.remove(0);
        }
        self.events.push(event);
    }

    /// Number of stored events.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Returns true if there are no events.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Replay (iterate) all stored events from oldest to newest.
    pub fn replay(&self) -> impl Iterator<Item = &Event> {
        self.events.iter()
    }

    /// Replay only events matching a filter.
    pub fn replay_filtered(&self, filter: &EventFilter) -> Vec<&Event> {
        self.events.iter().filter(|e| filter.matches(e)).collect()
    }

    /// Return the most recent event, if any.
    pub fn last(&self) -> Option<&Event> {
        self.events.last()
    }
}

impl Default for EventHistory {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// EventBus
// ---------------------------------------------------------------------------

/// Central publish/subscribe dispatcher.
pub struct EventBus {
    subscriptions: HashMap<SubscriberId, Subscription>,
    next_id: SubscriberId,
    history: EventHistory,
}

impl EventBus {
    /// Create a new event bus with unbounded history.
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            next_id: 0,
            history: EventHistory::new(),
        }
    }

    /// Create a bus with bounded history.
    pub fn with_history_capacity(max_history: usize) -> Self {
        Self {
            subscriptions: HashMap::new(),
            next_id: 0,
            history: EventHistory::with_capacity(max_history),
        }
    }

    /// Subscribe to events matching a filter. Returns the subscriber ID.
    pub fn subscribe(&mut self, filter: EventFilter, callback: fn(&Event)) -> SubscriberId {
        let id = self.next_id;
        self.next_id += 1;
        self.subscriptions.insert(
            id,
            Subscription {
                id,
                filter,
                callback,
            },
        );
        id
    }

    /// Unsubscribe by ID. Returns true if it existed.
    pub fn unsubscribe(&mut self, id: SubscriberId) -> bool {
        self.subscriptions.remove(&id).is_some()
    }

    /// Publish an event. Notifies all matching subscribers and appends to history.
    pub fn publish(&mut self, event: Event) {
        let subs: Vec<&Subscription> = self.subscriptions.values().filter(|s| s.filter.matches(&event)).collect();
        for sub in subs {
            (sub.callback)(&event);
        }
        self.history.append(event);
    }

    /// Number of active subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Access the event history.
    pub fn history(&self) -> &EventHistory {
        &self.history
    }

    /// Replay all history events to a callback (useful for late subscribers).
    pub fn replay_to(&self, callback: fn(&Event)) {
        for event in self.history.replay() {
            callback(event);
        }
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CALL_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn counting_callback(_event: &Event) {
        CALL_COUNT.fetch_add(1, Ordering::SeqCst);
    }

    fn noop_callback(_event: &Event) {}

    fn reset_count() {
        CALL_COUNT.store(0, Ordering::SeqCst);
    }

    // --- Priority tests ---

    #[test]
    fn priority_ordering() {
        assert!(Priority::Low < Priority::Normal);
        assert!(Priority::Normal < Priority::Critical);
    }

    #[test]
    fn priority_equality() {
        assert_eq!(Priority::Normal, Priority::Normal);
        assert_ne!(Priority::Low, Priority::Critical);
    }

    // --- Event tests ---

    #[test]
    fn event_new_has_fields() {
        let e = Event::with_timestamp("test", "payload", Priority::Normal, "src", 100);
        assert_eq!(e.event_type, "test");
        assert_eq!(e.payload, "payload");
        assert_eq!(e.priority, Priority::Normal);
        assert_eq!(e.source, "src");
        assert_eq!(e.timestamp_ms, 100);
    }

    #[test]
    fn event_new_auto_timestamp() {
        let e = Event::new("t", "p", Priority::Low, "s");
        assert!(e.timestamp_ms > 0);
    }

    #[test]
    fn event_equality() {
        let a = Event::with_timestamp("x", "y", Priority::Critical, "z", 42);
        let b = Event::with_timestamp("x", "y", Priority::Critical, "z", 42);
        assert_eq!(a, b);
    }

    // --- EventFilter tests ---

    #[test]
    fn filter_matches_everything_by_default() {
        let f = EventFilter::new();
        let e = Event::with_timestamp("anything", "data", Priority::Low, "anyone", 0);
        assert!(f.matches(&e));
    }

    #[test]
    fn filter_by_event_type() {
        let f = EventFilter::new().event_type("click");
        let matching = Event::with_timestamp("click", "x", Priority::Normal, "ui", 0);
        let not_matching = Event::with_timestamp("keypress", "x", Priority::Normal, "ui", 0);
        assert!(f.matches(&matching));
        assert!(!f.matches(&not_matching));
    }

    #[test]
    fn filter_by_min_priority() {
        let f = EventFilter::new().min_priority(Priority::Normal);
        assert!(f.matches(&Event::with_timestamp("t", "p", Priority::Normal, "s", 0)));
        assert!(f.matches(&Event::with_timestamp("t", "p", Priority::Critical, "s", 0)));
        assert!(!f.matches(&Event::with_timestamp("t", "p", Priority::Low, "s", 0)));
    }

    #[test]
    fn filter_by_source() {
        let f = EventFilter::new().source("agent-1");
        assert!(f.matches(&Event::with_timestamp("t", "p", Priority::Low, "agent-1", 0)));
        assert!(!f.matches(&Event::with_timestamp("t", "p", Priority::Low, "agent-2", 0)));
    }

    #[test]
    fn filter_combined() {
        let f = EventFilter::new()
            .event_type("alert")
            .min_priority(Priority::Critical)
            .source("monitor");
        let good = Event::with_timestamp("alert", "disk full", Priority::Critical, "monitor", 0);
        let bad_type = Event::with_timestamp("info", "disk full", Priority::Critical, "monitor", 0);
        let bad_pri = Event::with_timestamp("alert", "disk full", Priority::Normal, "monitor", 0);
        let bad_src = Event::with_timestamp("alert", "disk full", Priority::Critical, "sensor", 0);
        assert!(f.matches(&good));
        assert!(!f.matches(&bad_type));
        assert!(!f.matches(&bad_pri));
        assert!(!f.matches(&bad_src));
    }

    // --- EventHistory tests ---

    #[test]
    fn history_append_and_len() {
        let mut h = EventHistory::new();
        assert!(h.is_empty());
        h.append(Event::with_timestamp("a", "p", Priority::Low, "s", 1));
        h.append(Event::with_timestamp("b", "p", Priority::Low, "s", 2));
        assert_eq!(h.len(), 2);
        assert!(!h.is_empty());
    }

    #[test]
    fn history_last() {
        let mut h = EventHistory::new();
        assert!(h.last().is_none());
        h.append(Event::with_timestamp("first", "p", Priority::Low, "s", 1));
        h.append(Event::with_timestamp("second", "p", Priority::Low, "s", 2));
        assert_eq!(h.last().unwrap().event_type, "second");
    }

    #[test]
    fn history_capacity_evicts_oldest() {
        let mut h = EventHistory::with_capacity(2);
        h.append(Event::with_timestamp("a", "p", Priority::Low, "s", 1));
        h.append(Event::with_timestamp("b", "p", Priority::Low, "s", 2));
        h.append(Event::with_timestamp("c", "p", Priority::Low, "s", 3));
        assert_eq!(h.len(), 2);
        assert_eq!(h.replay().next().unwrap().event_type, "b");
    }

    #[test]
    fn history_replay_filtered() {
        let mut h = EventHistory::new();
        h.append(Event::with_timestamp("click", "p", Priority::Low, "ui", 1));
        h.append(Event::with_timestamp("alert", "p", Priority::Critical, "mon", 2));
        h.append(Event::with_timestamp("click", "q", Priority::Normal, "ui", 3));
        let f = EventFilter::new().event_type("click");
        let results = h.replay_filtered(&f);
        assert_eq!(results.len(), 2);
    }

    // --- EventBus tests ---

    #[test]
    fn bus_subscribe_and_count() {
        let mut bus = EventBus::new();
        let id = bus.subscribe(EventFilter::new(), noop_callback);
        assert_eq!(bus.subscription_count(), 1);
        assert!(bus.unsubscribe(id));
        assert_eq!(bus.subscription_count(), 0);
        assert!(!bus.unsubscribe(id)); // already removed
    }

    #[test]
    fn bus_publish_notifies_matching() {
        reset_count();
        let mut bus = EventBus::new();
        let f = EventFilter::new().event_type("ping");
        bus.subscribe(f, counting_callback);
        bus.subscribe(EventFilter::new(), counting_callback); // matches everything

        let e = Event::with_timestamp("ping", "data", Priority::Normal, "test", 0);
        bus.publish(e);
        assert_eq!(CALL_COUNT.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn bus_publish_skips_non_matching() {
        reset_count();
        let mut bus = EventBus::new();
        let f = EventFilter::new().event_type("click");
        bus.subscribe(f, counting_callback);

        let e = Event::with_timestamp("keypress", "data", Priority::Normal, "test", 0);
        bus.publish(e);
        assert_eq!(CALL_COUNT.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn bus_stores_history() {
        let mut bus = EventBus::new();
        bus.publish(Event::with_timestamp("a", "p", Priority::Low, "s", 1));
        bus.publish(Event::with_timestamp("b", "p", Priority::Normal, "s", 2));
        assert_eq!(bus.history().len(), 2);
    }

    #[test]
    fn bus_replay_to_callback() {
        static REPLAY_COUNT: AtomicUsize = AtomicUsize::new(0);
        REPLAY_COUNT.store(0, Ordering::SeqCst);
        fn replay_cb(_e: &Event) {
            REPLAY_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        let mut bus = EventBus::new();
        bus.publish(Event::with_timestamp("x", "p", Priority::Low, "s", 1));
        bus.publish(Event::with_timestamp("y", "p", Priority::Low, "s", 2));
        bus.replay_to(replay_cb);
        assert_eq!(REPLAY_COUNT.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn bus_default() {
        let bus = EventBus::default();
        assert_eq!(bus.subscription_count(), 0);
        assert!(bus.history().is_empty());
    }
}
