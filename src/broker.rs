use std::collections::{HashMap, VecDeque};

use crate::error::BrokerError;
use crate::topic;

/// What the broker does when a subscriber's queue is at capacity and a new
/// message arrives for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Drop the oldest queued message to make room for the new one.
    DropOldest,
    /// Keep the queue as is and drop the new message instead.
    RejectNew,
}

/// Counters for one subscriber: how many messages it actually received
/// versus how many were dropped because its queue was full.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SubscriberStats {
    pub delivered: u64,
    pub dropped: u64,
}

/// Counters for one topic name as it was published to: how many
/// deliveries the publish produced across all matching subscribers, and
/// how many of those deliveries were dropped by a full queue.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TopicStats {
    pub delivered: u64,
    pub dropped: u64,
}

struct Subscriber {
    pattern: String,
    queue: VecDeque<Vec<u8>>,
    max_depth: usize,
    policy: OverflowPolicy,
    stats: SubscriberStats,
}

/// An in-memory publish/subscribe broker.
///
/// Every subscriber owns a bounded queue. Publishing to a topic delivers to
/// every current subscriber whose pattern matches, following the
/// [`OverflowPolicy`] each subscriber was created with when its queue is
/// full. Memory use is capped by `subscriber_count * max_depth`, it can
/// never grow past that no matter how slow a subscriber is.
pub struct Broker {
    subscribers: HashMap<u64, Subscriber>,
    next_id: u64,
    max_message_size: usize,
    topic_stats: HashMap<String, TopicStats>,
}

impl Broker {
    /// Create a broker that rejects any message body larger than
    /// `max_message_size` bytes.
    pub fn new(max_message_size: usize) -> Self {
        Broker {
            subscribers: HashMap::new(),
            next_id: 1,
            max_message_size,
            topic_stats: HashMap::new(),
        }
    }

    /// Subscribe to `pattern` (an exact topic or a `*`-wildcard pattern, see
    /// [`topic::matches`]) with a bounded queue of `max_depth` messages and
    /// the given overflow policy. Returns the new subscriber's id.
    pub fn subscribe(
        &mut self,
        pattern: &str,
        max_depth: usize,
        policy: OverflowPolicy,
    ) -> Result<u64, BrokerError> {
        if pattern.is_empty() {
            return Err(BrokerError::EmptyTopic);
        }
        let id = self.next_id;
        self.next_id += 1;
        self.subscribers.insert(
            id,
            Subscriber {
                pattern: pattern.to_string(),
                queue: VecDeque::new(),
                max_depth: max_depth.max(1),
                policy,
                stats: SubscriberStats::default(),
            },
        );
        Ok(id)
    }

    /// Remove a subscriber. After this call it receives no further
    /// messages and its queued messages are discarded.
    pub fn unsubscribe(&mut self, id: u64) -> Result<(), BrokerError> {
        self.subscribers
            .remove(&id)
            .map(|_| ())
            .ok_or(BrokerError::UnknownSubscriber(id))
    }

    /// Publish `message` to `topic`. Delivers to every current subscriber
    /// whose pattern matches the topic, applying that subscriber's
    /// overflow policy if its queue is full. Returns the number of
    /// subscribers the message was actually enqueued for (drops do not
    /// count).
    pub fn publish(&mut self, topic: &str, message: Vec<u8>) -> Result<usize, BrokerError> {
        if topic.is_empty() {
            return Err(BrokerError::EmptyTopic);
        }
        if message.len() > self.max_message_size {
            return Err(BrokerError::MessageTooLarge {
                size: message.len(),
                max: self.max_message_size,
            });
        }
        let stats = self.topic_stats.entry(topic.to_string()).or_default();
        let mut delivered_count = 0;
        for sub in self.subscribers.values_mut() {
            if !topic::matches(&sub.pattern, topic) {
                continue;
            }
            if sub.queue.len() >= sub.max_depth {
                match sub.policy {
                    OverflowPolicy::DropOldest => {
                        sub.queue.pop_front();
                        sub.queue.push_back(message.clone());
                        sub.stats.delivered += 1;
                        sub.stats.dropped += 1;
                        stats.delivered += 1;
                        stats.dropped += 1;
                        delivered_count += 1;
                    }
                    OverflowPolicy::RejectNew => {
                        sub.stats.dropped += 1;
                        stats.dropped += 1;
                    }
                }
            } else {
                sub.queue.push_back(message.clone());
                sub.stats.delivered += 1;
                stats.delivered += 1;
                delivered_count += 1;
            }
        }
        Ok(delivered_count)
    }

    /// Pop the next queued message for a subscriber, if any.
    pub fn receive(&mut self, id: u64) -> Result<Option<Vec<u8>>, BrokerError> {
        self.subscribers
            .get_mut(&id)
            .map(|sub| sub.queue.pop_front())
            .ok_or(BrokerError::UnknownSubscriber(id))
    }

    /// How many messages are currently queued for a subscriber.
    pub fn queue_len(&self, id: u64) -> Result<usize, BrokerError> {
        self.subscribers
            .get(&id)
            .map(|sub| sub.queue.len())
            .ok_or(BrokerError::UnknownSubscriber(id))
    }

    /// Delivered/dropped counters for one subscriber.
    pub fn subscriber_stats(&self, id: u64) -> Result<SubscriberStats, BrokerError> {
        self.subscribers
            .get(&id)
            .map(|sub| sub.stats)
            .ok_or(BrokerError::UnknownSubscriber(id))
    }

    /// Delivered/dropped counters for a topic name, as published to so far.
    pub fn topic_stats(&self, topic: &str) -> TopicStats {
        self.topic_stats.get(topic).copied().unwrap_or_default()
    }

    /// Number of currently active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivers_to_all_subscribers_of_topic_and_no_other_topic() {
        let mut b = Broker::new(1024);
        let a = b.subscribe("orders", 10, OverflowPolicy::RejectNew).unwrap();
        let c = b.subscribe("orders", 10, OverflowPolicy::RejectNew).unwrap();
        let other = b.subscribe("shipping", 10, OverflowPolicy::RejectNew).unwrap();

        b.publish("orders", b"hello".to_vec()).unwrap();

        assert_eq!(b.receive(a).unwrap(), Some(b"hello".to_vec()));
        assert_eq!(b.receive(c).unwrap(), Some(b"hello".to_vec()));
        assert_eq!(b.receive(other).unwrap(), None);
    }

    #[test]
    fn wildcard_delivers_only_to_matching_topics() {
        let mut b = Broker::new(1024);
        let s = b.subscribe("logs.*", 10, OverflowPolicy::RejectNew).unwrap();

        b.publish("logs.error", b"e".to_vec()).unwrap();
        b.publish("metrics.error", b"m".to_vec()).unwrap();

        assert_eq!(b.receive(s).unwrap(), Some(b"e".to_vec()));
        assert_eq!(b.receive(s).unwrap(), None);
    }

    #[test]
    fn bounded_queue_drops_oldest_at_capacity_instead_of_growing() {
        let mut b = Broker::new(1024);
        let s = b.subscribe("t", 2, OverflowPolicy::DropOldest).unwrap();

        b.publish("t", b"1".to_vec()).unwrap();
        b.publish("t", b"2".to_vec()).unwrap();
        b.publish("t", b"3".to_vec()).unwrap();

        assert_eq!(b.queue_len(s).unwrap(), 2);
        assert_eq!(b.receive(s).unwrap(), Some(b"2".to_vec()));
        assert_eq!(b.receive(s).unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn bounded_queue_rejects_new_at_capacity_instead_of_growing() {
        let mut b = Broker::new(1024);
        let s = b.subscribe("t", 2, OverflowPolicy::RejectNew).unwrap();

        b.publish("t", b"1".to_vec()).unwrap();
        b.publish("t", b"2".to_vec()).unwrap();
        b.publish("t", b"3".to_vec()).unwrap();

        assert_eq!(b.queue_len(s).unwrap(), 2);
        assert_eq!(b.receive(s).unwrap(), Some(b"1".to_vec()));
        assert_eq!(b.receive(s).unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn counters_reflect_delivered_and_dropped() {
        let mut b = Broker::new(1024);
        let s = b.subscribe("t", 1, OverflowPolicy::RejectNew).unwrap();

        b.publish("t", b"1".to_vec()).unwrap();
        b.publish("t", b"2".to_vec()).unwrap();
        b.publish("t", b"3".to_vec()).unwrap();

        let stats = b.subscriber_stats(s).unwrap();
        assert_eq!(stats.delivered, 1);
        assert_eq!(stats.dropped, 2);

        let tstats = b.topic_stats("t");
        assert_eq!(tstats.delivered, 1);
        assert_eq!(tstats.dropped, 2);
    }

    #[test]
    fn unsubscribe_stops_delivery() {
        let mut b = Broker::new(1024);
        let s = b.subscribe("t", 10, OverflowPolicy::RejectNew).unwrap();
        b.unsubscribe(s).unwrap();

        let result = b.publish("t", b"x".to_vec()).unwrap();
        assert_eq!(result, 0);
        assert!(matches!(b.receive(s), Err(BrokerError::UnknownSubscriber(_))));
    }

    #[test]
    fn message_too_large_is_rejected() {
        let mut b = Broker::new(4);
        let err = b.publish("t", b"way too big".to_vec()).unwrap_err();
        assert!(matches!(err, BrokerError::MessageTooLarge { .. }));
    }
}
