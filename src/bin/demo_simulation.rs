//! Self-contained simulation demonstrating the Rust Client SDK over local TCP sockets.

use match_engine::client::{ClientEvent, MatchEngineClient};
use match_engine::engine::run_matching_engine_loop;
use match_engine::gateway::{start_inbound_gateway, start_outbound_broadcaster};
use match_engine::ring_buffer::spsc_channel;
use match_engine::types::{OrderId, Price, Quantity, Side};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

fn main() {
    println!("=======================================================");
    println!("       Match Engine Client SDK Demonstration           ");
    println!("=======================================================");

    const CAPACITY: usize = 1024;
    let (inbound_producer, engine_cmd_consumer) = spsc_channel::<_, CAPACITY>();
    let (engine_event_producer, outbound_consumer) = spsc_channel::<_, CAPACITY>();

    let running = Arc::new(AtomicBool::new(true));

    let in_addr = "127.0.0.1:19001";
    let out_addr = "127.0.0.1:19002";

    let in_handle = start_inbound_gateway(in_addr, inbound_producer, Arc::clone(&running));
    let out_handle = start_outbound_broadcaster(out_addr, outbound_consumer, Arc::clone(&running));

    let r_engine = Arc::clone(&running);
    let engine_handle = thread::spawn(move || {
        run_matching_engine_loop(engine_cmd_consumer, engine_event_producer, r_engine);
    });

    thread::sleep(Duration::from_millis(50));

    // Connect using high-level MatchEngineClient
    let mut client =
        MatchEngineClient::connect(in_addr, out_addr).expect("Failed to connect MatchEngineClient");

    // Spawn background event consumer thread
    let subscriber_handle = thread::spawn(move || {
        while let Ok(event) = client.subscriber.read_event() {
            match event {
                ClientEvent::OrderAccepted {
                    sequence_id,
                    order_id,
                    resting,
                    ..
                } => {
                    println!(
                        " -> [SDK Event] Seq #{}: OrderAccepted (id={}, resting={})",
                        sequence_id, order_id.0, resting
                    );
                }
                ClientEvent::TradeExecuted {
                    sequence_id,
                    taker_order_id,
                    maker_order_id,
                    price,
                    quantity,
                    ..
                } => {
                    println!(
                        " -> [SDK Event] Seq #{}: TradeExecuted (taker={}, maker={}, qty={}, price={})",
                        sequence_id, taker_order_id.0, maker_order_id.0, quantity.0, price.0
                    );
                }
                ClientEvent::OrderModified {
                    sequence_id,
                    order_id,
                    new_quantity,
                    ..
                } => {
                    println!(
                        " -> [SDK Event] Seq #{}: OrderModified (id={}, new_qty={})",
                        sequence_id, order_id.0, new_quantity.0
                    );
                }
                ClientEvent::OrderCancelled {
                    sequence_id,
                    order_id,
                    ..
                } => {
                    println!(
                        " -> [SDK Event] Seq #{}: OrderCancelled (id={})",
                        sequence_id, order_id.0
                    );
                }
                ClientEvent::EngineShutdown { sequence_id, .. } => {
                    println!(
                        " -> [SDK Event] Seq #{}: EngineShutdown received.",
                        sequence_id
                    );
                    break;
                }
                _ => {}
            }
        }
    });

    // 1. Submit Resting Maker Sell Order
    println!("1. Placing resting Limit Sell: Order 1 @ 100 (qty: 50)");
    client
        .inbound
        .place_gtc_limit(OrderId(1), Side::Sell, Price(100), Quantity(50), 1001)
        .unwrap();
    client.inbound.flush().unwrap();
    thread::sleep(Duration::from_millis(10));

    // 2. Submit Crossing Taker Buy Order
    println!("2. Placing crossing Limit Buy: Order 2 @ 100 (qty: 20)");
    client
        .inbound
        .place_gtc_limit(OrderId(2), Side::Buy, Price(100), Quantity(20), 1002)
        .unwrap();
    client.inbound.flush().unwrap();
    thread::sleep(Duration::from_millis(10));

    // 3. Amend Resting Maker Order (reduce quantity to 15)
    println!("3. Amending resting Order 1 quantity down to 15");
    client
        .inbound
        .modify_order(OrderId(1), None, Quantity(15), 1003)
        .unwrap();
    client.inbound.flush().unwrap();
    thread::sleep(Duration::from_millis(10));

    // 4. Cancel Resting Maker Order
    println!("4. Cancelling resting Order 1");
    client.inbound.cancel_order(OrderId(1), 1004).unwrap();
    client.inbound.flush().unwrap();
    thread::sleep(Duration::from_millis(10));

    // 5. Clean Shutdown
    println!("5. Sending graceful shutdown");
    client.inbound.shutdown().unwrap();

    subscriber_handle.join().unwrap();

    running.store(false, Ordering::Relaxed);
    engine_handle.join().unwrap();
    in_handle.join().unwrap();
    out_handle.join().unwrap();

    println!("\n=======================================================");
    println!("       SDK Demonstration Completed Cleanly             ");
    println!("=======================================================");
}
