//! Example downstream Kafka Adapter demonstrating how to consume binary events from port 9002
//! and idempotently translate them into Kafka-compatible message payloads.

use std::io::Read;
use std::net::TcpStream;
use std::time::Duration;
use match_engine::wire::WireEventFrame;

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
    let mut stream = loop {
        match TcpStream::connect(target_addr) {
            Ok(s) => {
                println!("Connected to matching engine outbound stream!");
                break s;
            }
            Err(_) => {
                println!("Waiting for match engine to start on port 9002...");
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    };

    stream.set_nodelay(true).unwrap();

    let mut kafka_producer = MockKafkaProducer::new("exchange.market.events");
    let mut buf = [0u8; WireEventFrame::SIZE];

    loop {
        match stream.read_exact(&mut buf) {
            Ok(()) => {
                let frame = WireEventFrame::from_bytes(&buf);

                // Translate binary WireEventFrame into JSON/Kafka format
                let (event_name, key, json_payload) = match frame.event_type {
                    1 => (
                        "OrderAccepted",
                        frame.order_id,
                        format!(
                            "{{\"event\":\"OrderAccepted\",\"order_id\":{},\"resting\":{}}}",
                            frame.order_id, frame.leaves_quantity == 1
                        ),
                    ),
                    2 => (
                        "TradeExecuted",
                        frame.order_id,
                        format!(
                            "{{\"event\":\"TradeExecuted\",\"taker_order_id\":{},\"maker_order_id\":{},\"price\":{},\"quantity\":{}}}",
                            frame.order_id, frame.maker_order_id, frame.price, frame.quantity
                        ),
                    ),
                    3 => (
                        "OrderCancelled",
                        frame.order_id,
                        format!(
                            "{{\"event\":\"OrderCancelled\",\"order_id\":{}}}",
                            frame.order_id
                        ),
                    ),
                    4 => (
                        "OrderRejected",
                        frame.order_id,
                        format!(
                            "{{\"event\":\"OrderRejected\",\"order_id\":{}}}",
                            frame.order_id
                        ),
                    ),
                    5 => (
                        "L2Update",
                        frame.price,
                        format!(
                            "{{\"event\":\"L2Update\",\"side\":\"{}\",\"price\":{},\"new_qty\":{}}}",
                            if frame.maker_order_id == 0 { "Buy" } else { "Sell" },
                            frame.price,
                            frame.quantity
                        ),
                    ),
                    255 => {
                        println!("EngineShutdown received from match engine. Exiting cleanly.");
                        break;
                    }
                    _ => continue,
                };

                let _ = event_name;
                // Idempotently publish to Kafka topic
                let _ = kafka_producer.publish(frame.sequence_id, key, &json_payload);
            }
            Err(e) => {
                println!("Connection closed by engine or error: {:?}", e);
                break;
            }
        }
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
