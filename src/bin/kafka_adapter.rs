//! Example downstream Kafka Adapter demonstrating how to consume binary events from port 9002
//! and idempotently translate them into Kafka-compatible message payloads.

use match_engine::client::{ClientEvent, EventSubscriber};
use std::time::Duration;

/// Simulated Kafka Producer tracking published sequence IDs for Exactly-Once delivery.
pub struct MockKafkaProducer {
    pub topic: &'static str,
    pub last_published_seq: u64,
    pub published_count: u64,
}

impl MockKafkaProducer {
    pub fn new(topic: &'static str) -> Self {
        Self {
            topic,
            last_published_seq: 0,
            published_count: 0,
        }
    }

    /// Idempotently publish a message to the target topic.
    /// Rejects duplicates where sequence_id <= last_published_seq.
    pub fn publish(&mut self, seq: u64, key: u64, payload: &str) -> Result<(), &'static str> {
        if seq <= self.last_published_seq {
            println!(
                " [Kafka Deduplication] Skipped duplicate event with seq={} (last published={})",
                seq, self.last_published_seq
            );
            return Err("Duplicate sequence ID discarded");
        }

        self.last_published_seq = seq;
        self.published_count += 1;

        println!(
            " [Kafka Produce] Topic='{}' | PartitionKey={} | SeqId={} | Payload={}",
            self.topic, key, seq, payload
        );

        Ok(())
    }
}

fn main() {
    println!("=== Match Engine -> Kafka Bridge Adapter ===");
    println!("Connecting to Outbound Broadcaster at 127.0.0.1:9002...");

    let target_addr = "127.0.0.1:9002";
    let mut subscriber = loop {
        match EventSubscriber::connect(target_addr) {
            Ok(s) => {
                println!("Connected to matching engine outbound stream via Client SDK!");
                break s;
            }
            Err(_) => {
                println!("Waiting for match engine to start on port 9002...");
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    };

    let mut kafka_producer = MockKafkaProducer::new("exchange.market.events");

    while let Ok(event) = subscriber.read_event() {
        let (seq, key, json_payload) = match event {
            ClientEvent::OrderAccepted { sequence_id, order_id, resting, .. } => (
                sequence_id,
                order_id.0,
                format!("{{\"event\":\"OrderAccepted\",\"order_id\":{},\"resting\":{}}}", order_id.0, resting),
            ),
            ClientEvent::TradeExecuted { sequence_id, taker_order_id, maker_order_id, price, quantity, .. } => (
                sequence_id,
                taker_order_id.0,
                format!(
                    "{{\"event\":\"TradeExecuted\",\"taker_order_id\":{},\"maker_order_id\":{},\"price\":{},\"quantity\":{}}}",
                    taker_order_id.0, maker_order_id.0, price.0, quantity.0
                ),
            ),
            ClientEvent::OrderModified { sequence_id, order_id, new_price, new_quantity, .. } => (
                sequence_id,
                order_id.0,
                format!(
                    "{{\"event\":\"OrderModified\",\"order_id\":{},\"new_price\":{},\"new_qty\":{}}}",
                    order_id.0, new_price.0, new_quantity.0
                ),
            ),
            ClientEvent::OrderCancelled { sequence_id, order_id, .. } => (
                sequence_id,
                order_id.0,
                format!("{{\"event\":\"OrderCancelled\",\"order_id\":{}}}", order_id.0),
            ),
            ClientEvent::OrderRejected { sequence_id, order_id, .. } => (
                sequence_id,
                order_id.0,
                format!("{{\"event\":\"OrderRejected\",\"order_id\":{}}}", order_id.0),
            ),
            ClientEvent::EngineShutdown { sequence_id, .. } => {
                println!("EngineShutdown event #{} received from match engine. Exiting cleanly.", sequence_id);
                break;
            }
            ClientEvent::Unknown { sequence_id, event_type } => (
                sequence_id,
                0,
                format!("{{\"event\":\"Unknown\",\"type\":{}}}", event_type),
            ),
        };

        let _ = kafka_producer.publish(seq, key, &json_payload);
    }

    println!(
        "Kafka Adapter finished. Total unique messages published: {}",
        kafka_producer.published_count
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_producer_idempotency() {
        let mut producer = MockKafkaProducer::new("test.topic");

        assert!(producer.publish(1, 101, "{\"trade\": 1}").is_ok());
        assert!(producer.publish(2, 102, "{\"trade\": 2}").is_ok());

        // Replayed duplicate event (seq 1 and 2) must be rejected
        assert!(producer.publish(1, 101, "{\"trade\": 1}").is_err());
        assert!(producer.publish(2, 102, "{\"trade\": 2}").is_err());

        // Forward progress continues cleanly
        assert!(producer.publish(3, 103, "{\"trade\": 3}").is_ok());
        assert_eq!(producer.published_count, 3);
    }
}
