//! Riverbed: a small in-memory publish/subscribe message broker.
//!
//! Topics are plain dot-separated names. Subscribers register with an
//! exact topic or a `*`-wildcard pattern (see [`topic::matches`]) and get a
//! bounded queue, so a slow subscriber can never make the broker's memory
//! use grow without bound. See [`broker::Broker`] for the entry point.

pub mod broker;
pub mod error;
pub mod topic;

pub use broker::{Broker, OverflowPolicy, SubscriberStats, TopicStats};
pub use error::BrokerError;
