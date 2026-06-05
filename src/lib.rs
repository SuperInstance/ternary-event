#![forbid(unsafe_code)]
//! ternary-event — Pub/sub event dispatch for ternary agents

use std::collections::HashMap;

/// Ternary priority level for events
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Low = -1,
    Normal = 0,
    High = 1,
}

impl Priority {
    pub fn from_i8(v: i8) -> Self {
        match v { -1 => Self::Low, 1 => Self::High, _ => Self::Normal }
    }
    pub fn as_i8(&self) -> i8 { *self as i8 }
}

/// A typed event with payload
#[derive(Debug, Clone)]
pub struct Event {
    pub event_type: String,
    pub priority: Priority,
    pub source: u64,
    pub payload: HashMap<String, String>,
}

impl Event {
    pub fn new(event_type: &str, source: u64) -> Self {
        Self { event_type: event_type.to_string(), priority: Priority::Normal, source, payload: HashMap::new() }
    }
    pub fn with_priority(mut self, p: Priority) -> Self { self.priority = p; self }
    pub fn with_payload(mut self, key: &str, value: &str) -> Self { self.payload.insert(key.to_string(), value.to_string()); self }
}

/// Filter for event subscriptions
#[derive(Debug, Clone)]
pub enum EventFilter {
    All,
    Type(String),
    Source(u64),
    PriorityMin(Priority),
    And(Box<EventFilter>, Box<EventFilter>),
    Or(Box<EventFilter>, Box<EventFilter>),
}

impl EventFilter {
    pub fn matches(&self, event: &Event) -> bool {
        match self {
            Self::All => true,
            Self::Type(t) => event.event_type == *t,
            Self::Source(s) => event.source == *s,
            Self::PriorityMin(p) => event.priority >= *p,
            Self::And(a, b) => a.matches(event) && b.matches(event),
            Self::Or(a, b) => a.matches(event) || b.matches(event),
        }
    }
}

/// Subscription ID
pub type SubId = u64;

/// A subscription with optional filter
pub struct Subscription {
    pub id: SubId,
    pub filter: EventFilter,
    pub handler: fn(&Event),
}

/// Append-only event log with replay capability
pub struct EventHistory {
    events: Vec<Event>,
    max_size: usize,
}

impl EventHistory {
    pub fn new(max_size: usize) -> Self { Self { events: Vec::new(), max_size } }
    pub fn append(&mut self, event: Event) {
        if self.events.len() >= self.max_size { self.events.remove(0); }
        self.events.push(event);
    }
    pub fn replay(&self, handler: fn(&Event)) { for e in &self.events { handler(e); } }
    pub fn len(&self) -> usize { self.events.len() }
    pub fn is_empty(&self) -> bool { self.events.is_empty() }
    pub fn events(&self) -> &[Event] { &self.events }
    pub fn by_type(&self, t: &str) -> Vec<&Event> { self.events.iter().filter(|e| e.event_type == t).collect() }
    pub fn by_source(&self, s: u64) -> Vec<&Event> { self.events.iter().filter(|e| e.source == s).collect() }
}

/// Convenience trait for event emitters
pub trait EventEmitter {
    fn emit(&mut self, event: Event);
}

/// The central event bus
pub struct EventBus {
    subscriptions: HashMap<SubId, Subscription>,
    next_id: SubId,
    history: EventHistory,
}

impl EventBus {
    pub fn new(history_size: usize) -> Self {
        Self { subscriptions: HashMap::new(), next_id: 0, history: EventHistory::new(history_size) }
    }

    pub fn subscribe(&mut self, filter: EventFilter, handler: fn(&Event)) -> SubId {
        let id = self.next_id;
        self.next_id += 1;
        self.subscriptions.insert(id, Subscription { id, filter, handler });
        id
    }

    pub fn subscribe_all(&mut self, handler: fn(&Event)) -> SubId {
        self.subscribe(EventFilter::All, handler)
    }

    pub fn unsubscribe(&mut self, id: SubId) -> bool { self.subscriptions.remove(&id).is_some() }

    pub fn publish(&mut self, event: Event) {
        self.history.append(event.clone());
        for sub in self.subscriptions.values() {
            if sub.filter.matches(&event) {
                (sub.handler)(&event);
            }
        }
    }

    pub fn subscription_count(&self) -> usize { self.subscriptions.len() }
    pub fn history(&self) -> &EventHistory { &self.history }
}

impl EventEmitter for EventBus {
    fn emit(&mut self, event: Event) { self.publish(event); }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    fn count_handler(_: &Event) { COUNTER.fetch_add(1, Ordering::SeqCst); }
    fn reset() { COUNTER.store(0, Ordering::SeqCst); }

    #[test] fn priority_values() {
        assert_eq!(Priority::Low.as_i8(), -1);
        assert_eq!(Priority::Normal.as_i8(), 0);
        assert_eq!(Priority::High.as_i8(), 1);
    }

    #[test] fn priority_from_i8() {
        assert_eq!(Priority::from_i8(-1), Priority::Low);
        assert_eq!(Priority::from_i8(0), Priority::Normal);
        assert_eq!(Priority::from_i8(1), Priority::High);
        assert_eq!(Priority::from_i8(99), Priority::Normal);
    }

    #[test] fn event_builder() {
        let e = Event::new("move", 1).with_priority(Priority::High).with_payload("dir", "north");
        assert_eq!(e.event_type, "move");
        assert_eq!(e.priority, Priority::High);
        assert_eq!(e.payload.get("dir"), Some(&"north".to_string()));
    }

    #[test] fn filter_all() { assert!(EventFilter::All.matches(&Event::new("x", 0))); }

    #[test] fn filter_type() {
        let f = EventFilter::Type("move".into());
        assert!(f.matches(&Event::new("move", 0)));
        assert!(!f.matches(&Event::new("stay", 0)));
    }

    #[test] fn filter_source() {
        let f = EventFilter::Source(42);
        assert!(f.matches(&Event::new("x", 42)));
        assert!(!f.matches(&Event::new("x", 99)));
    }

    #[test] fn filter_priority_min() {
        let f = EventFilter::PriorityMin(Priority::High);
        assert!(f.matches(&Event::new("x", 0).with_priority(Priority::High)));
        assert!(!f.matches(&Event::new("x", 0).with_priority(Priority::Low)));
    }

    #[test] fn filter_and() {
        let f = EventFilter::And(
            Box::new(EventFilter::Type("move".into())),
            Box::new(EventFilter::Source(1)),
        );
        assert!(f.matches(&Event::new("move", 1)));
        assert!(!f.matches(&Event::new("move", 2)));
        assert!(!f.matches(&Event::new("stay", 1)));
    }

    #[test] fn filter_or() {
        let f = EventFilter::Or(
            Box::new(EventFilter::Type("move".into())),
            Box::new(EventFilter::Type("look".into())),
        );
        assert!(f.matches(&Event::new("move", 0)));
        assert!(f.matches(&Event::new("look", 0)));
        assert!(!f.matches(&Event::new("stay", 0)));
    }

    #[test] fn history_append_and_query() {
        let mut h = EventHistory::new(100);
        h.append(Event::new("move", 1));
        h.append(Event::new("look", 2));
        h.append(Event::new("move", 3));
        assert_eq!(h.len(), 3);
        assert_eq!(h.by_type("move").len(), 2);
        assert_eq!(h.by_source(2).len(), 1);
    }

    #[test] fn history_max_size() {
        let mut h = EventHistory::new(2);
        h.append(Event::new("a", 0));
        h.append(Event::new("b", 0));
        h.append(Event::new("c", 0));
        assert_eq!(h.len(), 2);
        assert_eq!(h.events()[0].event_type, "b");
    }

    #[test] fn history_replay() {
        reset();
        let mut h = EventHistory::new(100);
        h.append(Event::new("x", 0));
        h.append(Event::new("y", 0));
        h.replay(count_handler);
        assert_eq!(COUNTER.load(Ordering::SeqCst), 2);
    }

    #[test] fn bus_publish_and_subscribe() {
        reset();
        let mut bus = EventBus::new(100);
        bus.subscribe_all(count_handler);
        bus.publish(Event::new("a", 0));
        bus.publish(Event::new("b", 0));
        assert_eq!(COUNTER.load(Ordering::SeqCst), 2);
        assert_eq!(bus.history().len(), 2);
    }

    #[test] fn bus_filtered_subscription() {
        reset();
        let mut bus = EventBus::new(100);
        bus.subscribe(EventFilter::Type("move".into()), count_handler);
        bus.publish(Event::new("move", 0));
        bus.publish(Event::new("look", 0));
        assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
    }

    #[test] fn bus_unsubscribe() {
        reset();
        let mut bus = EventBus::new(100);
        let id = bus.subscribe_all(count_handler);
        bus.publish(Event::new("a", 0));
        assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
        bus.unsubscribe(id);
        bus.publish(Event::new("b", 0));
        assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
    }

    #[test] fn bus_multiple_subscribers() {
        static C2: AtomicUsize = AtomicUsize::new(0);
        reset();
        let mut bus = EventBus::new(100);
        bus.subscribe_all(count_handler);
        bus.subscribe_all(|_| { C2.fetch_add(1, Ordering::SeqCst); });
        bus.publish(Event::new("x", 0));
        assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
        assert_eq!(C2.load(Ordering::SeqCst), 1);
        assert_eq!(bus.subscription_count(), 2);
    }
}
