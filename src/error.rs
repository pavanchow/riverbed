use std::fmt;

/// Errors the broker can return. The broker never panics; every failure path
/// is one of these variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerError {
    /// The message body exceeded the broker's configured max size.
    MessageTooLarge { size: usize, max: usize },
    /// A publish or unsubscribe named a subscriber id that does not exist.
    UnknownSubscriber(u64),
    /// A topic name or subscription pattern was empty.
    EmptyTopic,
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrokerError::MessageTooLarge { size, max } => {
                write!(f, "message size {size} exceeds max {max}")
            }
            BrokerError::UnknownSubscriber(id) => write!(f, "unknown subscriber id {id}"),
            BrokerError::EmptyTopic => write!(f, "topic name must not be empty"),
        }
    }
}

impl std::error::Error for BrokerError {}
