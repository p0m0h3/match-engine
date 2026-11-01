//! High-Performance Continuous Match Engine Daemon Server.
//! Runs continuously until interrupted by SIGINT / SIGTERM / Ctrl+C.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use match_engine::engine::run_matching_engine_loop;
use match_engine::gateway::{start_inbound_gateway, start_outbound_broadcaster};
use match_engine::ring_buffer::spsc_channel;

fn main() {
    println!("=======================================================");
    println!("         High-Performance Match Engine Daemon          ");
    println!("=======================================================");

    const CAPACITY: usize = 65_536;
    let (inbound_producer, engine_cmd_consumer) = spsc_channel::<_, CAPACITY>();
    let (engine_event_producer, outbound_consumer) = spsc_channel::<_, CAPACITY>();

    let running = Arc::new(AtomicBool::new(true));

    let in_addr = "0.0.0.0:9001";
    let out_addr = "0.0.0.0:9002";

    // 1. Start Inbound Gateway (Listens for OMS connections)
    let in_handle = start_inbound_gateway(in_addr, inbound_producer, Arc::clone(&running));
    println!(" [Inbound Gateway]     Listening on {} (Orders)", in_addr);

    // 2. Start Outbound Broadcaster (Listens for Kafka Adapter / Subscribers)
    let out_handle = start_outbound_broadcaster(out_addr, outbound_consumer, Arc::clone(&running));
    println!(" [Outbound Stream]     Listening on {} (Events)", out_addr);

    // 3. Start Core Matching Engine Thread (Single-Writer)
    let engine_handle = thread::spawn(move || {
        println!(" [Engine Core]         Single-writer matching loop pinned and running.");
        run_matching_engine_loop(engine_cmd_consumer, engine_event_producer);
        println!(" [Engine Core]         Matching loop terminated.");
    });

    println!("-------------------------------------------------------");
    println!(" Engine is running continuously. Press Ctrl+C to stop.");
    println!("-------------------------------------------------------\n");

    // 4. Register Ctrl+C signal handler for graceful shutdown
    let r_clone = Arc::clone(&running);

    ctrlc::set_handler(move || {
        println!("\n[Signal] Shutdown signal received. Exiting gracefully...");
        r_clone.store(false, Ordering::Relaxed);
    })
    .expect("Error setting Ctrl-C handler");

    // 5. Keep main thread alive while running
    while running.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(200));
    }

    println!("[Daemon] Waiting for background threads to terminate...");
    let _ = engine_handle.join();
    let _ = in_handle.join();
    let _ = out_handle.join();

    println!("=======================================================");
    println!("         Match Engine Daemon Exited Cleanly            ");
    println!("=======================================================");
}
