# Design

## Topics

A topic is a plain string, conventionally dot-separated segments such as `orders.created` or `logs.error`. The broker does not enforce a naming scheme beyond that patterns and topics are compared segment by segment.

## Subscriptions and matching

A subscriber registers a pattern, which is either an exact topic name or a pattern containing `*` segments. Matching is done in `src/topic.rs`:

- The pattern and the topic are split on `.`.
- They must have the same number of segments. `logs.*` does not match `logs` and does not match `logs.error.detail`.
- Each segment must be equal, except a pattern segment of exactly `*`, which matches any single topic segment in that position.
- An empty pattern or empty topic never matches anything.

This keeps matching a single linear pass with no precedence rules to reason about: a topic either matches a pattern or it does not, and multiple subscribers can independently match the same publish.

## Delivery

`Broker::publish(topic, message)` walks every current subscriber, checks its pattern against the topic, and for each match attempts to enqueue the message onto that subscriber's own queue. Delivery is synchronous and in-process, there is no separate delivery thread or network hop. Subscribers pull with `Broker::receive(id)`, which pops the oldest queued message.

## Bounded queues and the backpressure policy

Every subscriber is created with a `max_depth` and an `OverflowPolicy`:

- **`DropOldest`**: if the queue is at `max_depth` when a new message arrives, the oldest queued message is evicted and the new one is appended. The subscriber always has the freshest messages, at the cost of silently losing old ones. Both the eviction and the new arrival count in the subscriber's and topic's counters, the eviction as a drop, the arrival as a delivery.
- **`RejectNew`**: if the queue is at `max_depth`, the new message is discarded and the queue is left untouched. The subscriber keeps the oldest messages it has not yet consumed, at the cost of losing new ones during an overload. This counts as a drop and nothing else changes.

Either way, a subscriber's queue length can never exceed `max_depth`, so the broker's total memory for a subscriber's backlog is capped at `max_depth * message_size` regardless of how slowly that subscriber calls `receive`. A slow or stalled subscriber affects only itself, it cannot grow the broker's memory or slow down delivery to other subscribers, since each subscriber's queue and policy are independent.

## Counters

Two counter sets are tracked:

- **Per subscriber** (`SubscriberStats`): how many messages that subscriber actually received into its queue (`delivered`) versus how many were dropped for it because its queue was full (`dropped`).
- **Per topic** (`TopicStats`): the same two numbers summed across every subscriber a publish to that topic reached, so you can see at a glance whether a topic is experiencing backpressure anywhere downstream.

## Unsubscribe

`Broker::unsubscribe(id)` removes the subscriber entirely, including its queued and not-yet-received messages. Any later `publish` simply does not see that subscriber, and any later `receive` for that id returns `BrokerError::UnknownSubscriber`.

## Errors

The broker never panics. Every fallible operation returns `Result<_, BrokerError>`:

- `MessageTooLarge` when a published message exceeds the broker's configured max size.
- `UnknownSubscriber` when an operation names a subscriber id that does not exist, including one that has been unsubscribed.
- `EmptyTopic` when a subscribe pattern or publish topic is the empty string.

## What is deliberately out of scope

Riverbed is in-memory and single-process. There is no persistence, no clustering, no network protocol, and no ordering guarantee across topics, only within a single subscriber's own queue, since messages are appended in publish order and popped in that same order. These are the trade-offs that keep the whole broker small enough to read end to end.
