//! Self-contained mock simulation demonstrating end-to-end matching over local TCP sockets.

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
    println!("=== Match Engine Self-Test Simulation ===");

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

    // Simulated Subscriber
    let sub_handle = thread::spawn(move || {
        let mut sub = TcpStream::connect("127.0.0.1:19002").expect("Failed to connect to outbound port");
        sub.set_nodelay(true).unwrap();

        let mut buf = [0u8; WireEventFrame::SIZE];
        loop {
            if sub.read_exact(&mut buf).is_ok() {
                let ev = WireEventFrame::from_bytes(&buf);
                match ev.event_type {
                    1 => println!(" -> [Event Stream] Seq #{}: OrderAccepted (order_id={}, resting={})", ev.sequence_id, ev.order_id, ev.leaves_quantity == 1),
                    2 => println!(" -> [Event Stream] Seq #{}: TradeExecuted (taker={}, maker={}, qty={}, price={})", ev.sequence_id, ev.order_id, ev.maker_order_id, ev.quantity, ev.price),
                    255 => break,
                    _ => {}
                }
            } else {
                break;
            }
        }
    });

    // Simulated OMS
    thread::sleep(Duration::from_millis(50));
    let mut oms = TcpStream::connect("127.0.0.1:19001").expect("Failed to connect to inbound port");
    oms.set_nodelay(true).unwrap();

    let ask = WireCommandFrame::new_place_limit(1, 1, 100, 50, 1001);
    oms.write_all(ask.as_bytes()).unwrap();

    thread::sleep(Duration::from_millis(10));
    let buy = WireCommandFrame::new_place_limit(2, 0, 100, 20, 1002);
    oms.write_all(buy.as_bytes()).unwrap();

    thread::sleep(Duration::from_millis(50));
    let shutdown = WireCommandFrame::new_shutdown();
    oms.write_all(shutdown.as_bytes()).unwrap();
    oms.flush().unwrap();

    thread::sleep(Duration::from_millis(50));
    running.store(false, Ordering::Relaxed);

    sub_handle.join().unwrap();
    engine_handle.join().unwrap();
    in_handle.join().unwrap();
    out_handle.join().unwrap();

    println!("=== Simulation completed cleanly ===");
}
