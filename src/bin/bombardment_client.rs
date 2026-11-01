//! High-throughput TCP Bombardment Client for load and stress testing the matching engine.
//!
//! Can run standalone (spawning an in-process engine) or against an external running daemon server.

use std::collections::VecDeque;
use std::env;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::{Duration, Instant};

use match_engine::engine::run_matching_engine_loop;
use match_engine::gateway::{start_inbound_gateway, start_outbound_broadcaster};
use match_engine::ring_buffer::spsc_channel;
use match_engine::wire::{WireCommandFrame, WireEventFrame};

fn main() {
    std::panic::set_hook(Box::new(|info| {
        eprintln!(
            "\n[FATAL PANIC] thread '{:?}': {}\n",
            std::thread::current().name().unwrap_or("unnamed"),
            info
        );
    }));

    println!("=======================================================");
    println!("        TCP High-Velocity Bombardment Client           ");
    println!("=======================================================");

    let args: Vec<String> = env::args().collect();
    let num_orders: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(200_000);

    let default_in = "127.0.0.1:9001";
    let default_out = "127.0.0.1:9002";

    // Check if external daemon is running
    let external_server = TcpStream::connect(default_in).is_ok();

    let (in_addr, out_addr, _server_guards) = if external_server {
        println!(
            " [Target] Connected to existing Daemon on {} / {}",
            default_in, default_out
        );
        (default_in.to_string(), default_out.to_string(), None)
    } else {
        println!(" [Target] Spawning embedded high-capacity engine on 127.0.0.1:19001 / 19002...");
        let in_addr = "127.0.0.1:19001".to_string();
        let out_addr = "127.0.0.1:19002".to_string();

        const RING_CAPACITY: usize = 65_536;
        let (inbound_producer, engine_cmd_consumer) = spsc_channel::<_, RING_CAPACITY>();
        let (engine_event_producer, outbound_consumer) = spsc_channel::<_, RING_CAPACITY>();
        let running = Arc::new(AtomicBool::new(true));

        let in_handle = start_inbound_gateway(&in_addr, inbound_producer, Arc::clone(&running));
        let out_handle =
            start_outbound_broadcaster(&out_addr, outbound_consumer, Arc::clone(&running));

        let r_engine = Arc::clone(&running);
        let engine_handle = thread::spawn(move || {
            run_matching_engine_loop(engine_cmd_consumer, engine_event_producer, r_engine);
        });

        thread::sleep(Duration::from_millis(60));
        (
            in_addr,
            out_addr,
            Some((running, engine_handle, in_handle, out_handle)),
        )
    };

    println!(" [Workload] Bombarding with {} mixed orders...", num_orders);
    println!("   -> 40% Passive Maker Limits (book depth)");
    println!("   -> 35% Aggressive Taker Crossings (executions)");
    println!("   -> 15% Real-Time Order Cancellations");
    println!("   -> 10% Atomic Order Modifications (amend size/price)");
    println!("-------------------------------------------------------");

    // 1. Connect Outbound Subscriber Thread
    let out_target = out_addr.clone();
    let subscriber_handle = thread::spawn(move || {
        let stream = loop {
            match TcpStream::connect(&out_target) {
                Ok(s) => break s,
                Err(_) => thread::sleep(Duration::from_millis(10)),
            }
        };
        stream.set_nodelay(true).unwrap();
        let mut reader = BufReader::with_capacity(128 * 1024, stream);
        let mut buf = [0u8; WireEventFrame::SIZE];

        let mut count_accepted = 0u64;
        let mut count_trades = 0u64;
        let mut count_canceled = 0u64;
        let mut count_modified = 0u64;
        let mut count_rejected = 0u64;
        let mut total_events = 0u64;
        let mut last_seq = 0u64;
        let mut seq_errors = 0u64;

        loop {
            if reader.read_exact(&mut buf).is_err() {
                break;
            }

            let ev = WireEventFrame::from_bytes(&buf);
            total_events += 1;

            // Strict sequence monotonicity check
            if ev.sequence_id != last_seq + 1 {
                seq_errors += 1;
            }
            last_seq = ev.sequence_id;

            match ev.event_type {
                1 => count_accepted += 1,
                2 => count_trades += 1,
                3 => count_canceled += 1,
                4 => count_rejected += 1,
                6 => count_modified += 1,
                255 => break, // EngineShutdown
                _ => {}
            }
        }

        (
            total_events,
            last_seq,
            seq_errors,
            count_accepted,
            count_trades,
            count_canceled,
            count_modified,
            count_rejected,
        )
    });

    // 2. Connect Inbound Ingestion Stream
    let in_stream = loop {
        match TcpStream::connect(&in_addr) {
            Ok(s) => break s,
            Err(_) => thread::sleep(Duration::from_millis(10)),
        }
    };
    in_stream.set_nodelay(true).unwrap();
    let mut writer = BufWriter::with_capacity(128 * 1024, in_stream);

    // Track active resting orders for realistic cancel/modify targeting
    let mut active_resting_orders: VecDeque<u64> = VecDeque::with_capacity(50_000);

    let start_time = Instant::now();
    let mut sent_orders = 0usize;

    for i in 1..=num_orders {
        let order_id = i as u64;
        let selector = i % 100;

        let frame = if selector < 40 {
            // 40% Resting Maker orders (bids: 900..1000, asks: 1050..1150)
            let is_buy = (i % 2) == 0;
            let price = if is_buy {
                950 + (i as u64 % 50)
            } else {
                1050 + (i as u64 % 50)
            };
            let side = if is_buy { 0 } else { 1 };
            active_resting_orders.push_back(order_id);
            if active_resting_orders.len() > 40_000 {
                active_resting_orders.pop_front();
            }
            WireCommandFrame::new_place_limit(order_id, side, price, 10, i as u64)
        } else if selector < 75 {
            // 35% Aggressive Taker orders crossing the book
            let is_buy = (i % 2) == 0;
            let price = if is_buy { 1100 } else { 900 }; // crosses
            let side = if is_buy { 0 } else { 1 };
            WireCommandFrame::new_place_limit(order_id, side, price, 2, i as u64)
        } else if selector < 90 {
            // 15% Cancellations targeting a resting order
            if let Some(target_id) = active_resting_orders.pop_front() {
                WireCommandFrame::new_cancel(target_id, i as u64)
            } else {
                WireCommandFrame::new_place_limit(order_id, 0, 980, 5, i as u64)
            }
        } else {
            // 10% Modifications amending existing resting orders
            if let Some(&target_id) = active_resting_orders.front() {
                // Reduce quantity in-place
                WireCommandFrame::new_modify(target_id, 0, 5, i as u64)
            } else {
                WireCommandFrame::new_place_limit(order_id, 1, 1060, 5, i as u64)
            }
        };

        if let Err(e) = writer.write_all(frame.as_bytes()) {
            panic!(
                "Failed to write order {} (msg_type={}): {:?}",
                i, frame.msg_type, e
            );
        }
        sent_orders += 1;
        if i % 25_000 == 0 {
            println!("   ... sent {} orders", i);
        }
    }

    // Flush remaining buffered commands
    writer.flush().expect("Failed to flush buffer");

    // Send Shutdown frame to conclude benchmark
    let shutdown_frame = WireCommandFrame::new_shutdown();
    writer
        .write_all(shutdown_frame.as_bytes())
        .expect("Failed to send shutdown");
    writer.flush().expect("Failed to flush shutdown");

    let (
        total_events,
        last_seq,
        seq_errors,
        count_accepted,
        count_trades,
        count_canceled,
        count_modified,
        count_rejected,
    ) = subscriber_handle
        .join()
        .expect("Subscriber thread panicked");

    let total_duration = start_time.elapsed();
    let secs = total_duration.as_secs_f64();
    let orders_per_sec = sent_orders as f64 / secs;
    let events_per_sec = total_events as f64 / secs;

    println!("\n=== Bombardment Results ({} Orders) ===", sent_orders);
    println!(
        " Total Duration:          {:.3} s ({:.2} ms)",
        secs,
        secs * 1000.0
    );
    println!(
        " Ingestion Rate (Orders): {:>10.0} orders/sec",
        orders_per_sec
    );
    println!(
        " Egress Rate (Events):    {:>10.0} events/sec",
        events_per_sec
    );
    println!(" Total Events Emitted:    {:>10}", total_events);
    println!(" Last Sequence ID:        {:>10}", last_seq);
    println!(
        " Sequence Errors:         {:>10} {}",
        seq_errors,
        if seq_errors == 0 {
            "✓ (Strictly Monotonic)"
        } else {
            "✗ SEQUENCE GAP DETECTED!"
        }
    );
    println!("-------------------------------------------------------");
    println!(" Event Breakdown:");
    println!("   - OrderAccepted:       {:>10}", count_accepted);
    println!("   - TradeExecuted:       {:>10}", count_trades);
    println!("   - OrderCancelled:      {:>10}", count_canceled);
    println!("   - OrderModified:       {:>10}", count_modified);
    println!("   - OrderRejected:       {:>10}", count_rejected);
    println!("=======================================================");

    assert_eq!(seq_errors, 0, "Sequence check failed!");
    assert!(total_events > 0, "No events were received!");
    println!(" ✓ Bombardment Load Test Passed Successfully.\n");
}
