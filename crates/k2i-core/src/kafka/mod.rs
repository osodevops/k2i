//! Kafka consumer with backpressure support and exponential backoff.

mod consumer;
mod offset;

pub use consumer::{
    KafkaConsumerBuilder, KafkaMessage, PollResult, RetryConfig, SmartKafkaConsumer,
};
pub use offset::OffsetTracker;
