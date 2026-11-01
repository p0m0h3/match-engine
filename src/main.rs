use match_engine::engine::run_matching_engine_loop;
use match_engine::gateway::{start_inbound_gateway, start_outbound_broadcaster};
use match_engine::ring_buffer::spsc_channel;
use match_engine::wire::{WireCommandFrame, WireEventFrame};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() {
    println!("=== High-Performance Dual-Port Match Engine ===");

    const CAPACITY: usize = 1024;
    let (inbound_producer, engine_cmd_consumer) = spsc_channel::<_, CAPACITY>();
    let (engine_event_producer, outbound_consumer) = spsc_channel::<_, CAPACITY>();

    let running = Arc::new(AtomicBool::new(true));

    let in_addr = "127.0.0.1:9001";
    let out_addr = "127.0.0.1:9002";

    // 1. Boot Inbound Gateway (Port 9001: For OMS Order Ingestion)
    let in_handle = start_inbound_gateway(in_addr, inbound_producer, Arc::clone(&running));
    println!("[Inbound Gateway] Listening on {}", in_addr);

    // 2. Boot Outbound Broadcaster (Port 9002: For Kafka Translator / Market Data)
    let out_handle = start_outbound_broadcaster(out_addr, outbound_consumer, Arc::clone(&running));
    println!("[Outbound Broadcaster] Listening on {}", out_addr);

    // 3. Boot Core Matching Thread (Single-Writer)
    let engine_handle = thread::spawn(move || {
        println!("[Engine Core] Single-writer matching loop started.");
        run_matching_engine_loop(engine_cmd_consumer, engine_event_producer);
        println!("[Engine Core] Matching loop terminated.");
    });

    thread::sleep(Duration::from_millis(50));

    // 4. Simulate External Kafka Translator connecting to Outbound Port
    let translator_thread = thread::spawn(move || {
        println!("[Kafka Translator] Connecting to Outbound port 9002...");
        let mut sub = TcpStream::connect("127.0.0.1:9002").expect("Failed to connect to outbound port");
        sub.set_nodelay(true).unwrap();
        println!("[Kafka Translator] Connected! Listening for trade and execution events:");

        let mut buf = [0u8; WireEventFrame::SIZE];
        loop {
            if sub.read_exact(&mut buf).is_ok() {
                let ev = WireEventFrame::from_bytes(&buf);
                match ev.event_type {
                    1 => println!(
                        " -> [Kafka Translator] Seq #{}: OrderAccepted (order_id={}, resting={})",
                        ev.sequence_id, ev.order_id, ev.leaves_quantity == 1
                    ),
                    2 => println!(
                        " -> [Kafka Translator] Seq #{}: TradeExecuted (taker={}, maker={}, qty={}, price={})",
                        ev.sequence_id, ev.order_id, ev.maker_order_id, ev.quantity, ev.price
                    ),
                    3 => println!(
                        " -> [Kafka Translator] Seq #{}: OrderCancelled (order_id={})",
                        ev.sequence_id, ev.order_id
                    ),
                    255 => {
                        println!(" -> [Kafka Translator] EngineShutdown received. Disconnecting.");
                        break;
                    }
                    _ => {}
                }
            } else {
                break;
            }
        }
    });

    // 5. Simulate External OMS sending orders over Inbound Port
    thread::sleep(Duration::from_millis(50));
    println!("[OMS Client] Connecting to Inbound port 9001...");
    let mut oms = TcpStream::connect("127.0.0.1:9001").expect("Failed to connect to inbound port");
    oms.set_nodelay(true).unwrap();

    println!("[OMS Client] Placing resting Sell order (50 units @ price 100)...");
    let ask = WireCommandFrame::new_place_limit(1, 1, 100, 50, 1001);
    oms.write_all(ask.as_bytes()).unwrap();

    thread::sleep(Duration::from_millis(10));

    println!("[OMS Client] Placing aggressive crossing Buy order (20 units @ price 100)...");
    let buy = WireCommandFrame::new_place_limit(2, 0, 100, 20, 1002);
    oms.write_all(buy.as_bytes()).unwrap();

    thread::sleep(Duration::from_millis(50));

    println!("[OMS Client] Sending Shutdown signal...");
    let shutdown = WireCommandFrame::new_shutdown();
    oms.write_all(shutdown.as_bytes()).unwrap();
    oms.flush().unwrap();

    thread::sleep(Duration::from_millis(50));
    running.store(false, Ordering::Relaxed);
    translator_thread.join().unwrap();
    engine_handle.join().unwrap();
    in_handle.join().unwrap();
    out_handle.join().unwrap();

    println!("=== Dual-Port Match Engine finished cleanly ===");
}
