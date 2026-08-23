# Riverbed

**A message broker in Rust you can read end to end, with bounded queues so a slow subscriber never grows the broker's memory without limit.**

Riverbed is a from-scratch in-memory publish/subscribe message broker. No queue framework, no external broker dependency, just topics, subscriptions, and bounded per-subscriber queues, small enough to read in one sitting.

## What it is

- **Topics and subscribers.** Publish a message to a topic and it is delivered to every current subscriber of that topic.
- **Wildcard matching.** Patterns like `logs.*` match one segment in that position, so `logs.*` matches `logs.error` and `logs.info` but not `logs` or `logs.error.detail`. See `DESIGN.md` for the full rule.
- **Bounded queues.** Every subscriber has a maximum queue depth. When a subscriber's queue is full, the broker either drops the oldest queued message or rejects the new one, per a policy chosen at subscribe time. Memory use is capped at `subscriber_count * max_depth`, it cannot grow without bound no matter how slow a subscriber is.
- **Counters.** Delivered and dropped counts are tracked per subscriber and per topic, so backpressure is visible, not silent.
- **Subscribe and unsubscribe.** After unsubscribing, a subscriber receives nothing further.
- **Typed errors, no panics.** Oversized messages, unknown subscriber ids, and empty topics all return a typed `BrokerError` instead of panicking.
- **Capped message size.** The broker is constructed with a maximum message size and rejects anything larger.

## Usage

```
cargo build
cargo run -- demo
```

`riverbed demo` runs a scripted scenario: three subscribers on `logs.error` and `logs.*`, one of them deliberately slow with a small queue depth, twenty messages published, and the delivered and dropped counts printed at the end so backpressure is visible.

As a library:

```rust
use riverbed::{Broker, OverflowPolicy};

let mut broker = Broker::new(4096); // max message size in bytes
let id = broker.subscribe("logs.*", 100, OverflowPolicy::DropOldest)?;
broker.publish("logs.error", b"disk full".to_vec())?;
while let Some(msg) = broker.receive(id)? {
    println!("{:?}", msg);
}
broker.unsubscribe(id)?;
```

## Tests

```
cargo test
```

Covers exact and wildcard delivery, that a bounded queue drops or rejects at capacity rather than growing, that delivered and dropped counters are correct, and that unsubscribed subscribers receive nothing.

## Why

Most message brokers you reach for are a separate process with a wire protocol. Riverbed is the other end of that spectrum: an in-process broker you can read start to finish in a few minutes, with the one property that actually matters for reliability, bounded memory under a slow consumer, made explicit and tested.

By Pavan Nallamothu.
