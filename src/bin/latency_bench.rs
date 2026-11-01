//! Dedicated benchmark measuring Tick-to-Trade (T2T) latency distributions.

use match_engine::engine::{run_matching_engine_loop, EngineCommand, EngineEvent};
use match_engine::latency::{cycles_to_nanos, hardware_timer_frequency_hz, read_hardware_counter, LatencyHistogram};
use match_engine::ring_buffer::spsc_channel;
use match_engine::types::{Order, OrderId, Price, Quantity, Side};
use std::thread;

fn main() {
    println!("=== Hardware Tick-to-Trade (T2T) Latency Profiler ===");

    const NUM_ORDERS: usize = 100_000;
    const CAPACITY: usize = 2048;

    let (mut cmd_producer, engine_cmd_consumer) = spsc_channel::<EngineCommand, CAPACITY>();
    let (engine_event_producer, mut outbound_consumer) = spsc_channel::<EngineEvent, CAPACITY>();

    let freq = hardware_timer_frequency_hz();
    println!("Hardware Timer Frequency: {:.2} MHz", freq as f64 / 1_000_000.0);

    // Spawn matching engine on dedicated thread
    let engine_handle = thread::spawn(move || {
        run_matching_engine_loop(engine_cmd_consumer, engine_event_producer);
    });

    let mut histogram = LatencyHistogram::new();

    // Seed book with resting asks: 50 orders at prices 1000..1050
    for i in 1..=50 {
        let ask = Order::new_limit(
            OrderId(i as u64),
            Side::Sell,
            Price(1000 + (i as u64)),
            Quantity(1_000_000), // Deep liquidity
        );
        while cmd_producer.try_push(EngineCommand::PlaceOrder(ask)).is_err() {
            core::hint::spin_loop();
        }
    }

    // Drain initial confirmation events
    let mut drained = 0;
    while drained < 50 {
        if outbound_consumer.try_pop().is_some() {
            drained += 1;
        } else {
            core::hint::spin_loop();
        }
    }

    println!("Warm-up complete. Profiling {} crossing trades across threads...", NUM_ORDERS);

    // Profile T2T:
    // T0: Taken immediately before pushing aggressive buy command into SPSC ring buffer
    // T1: Taken immediately upon popping the resulting trade execution from outbound SPSC ring buffer
    for i in 1..=NUM_ORDERS {
        let taker_order = Order::new_limit(
            OrderId((100_000 + i) as u64),
            Side::Buy,
            Price(1050), // Crosses asks
            Quantity(1),
        );

        let t0 = read_hardware_counter();

        // Push order
        while cmd_producer.try_push(EngineCommand::PlaceOrder(taker_order)).is_err() {
            core::hint::spin_loop();
        }

        // Wait for execution event
        loop {
            if let Some(event) = outbound_consumer.try_pop() {
                if matches!(event, EngineEvent::TradeExecuted(_)) {
                    let t1 = read_hardware_counter();
                    let elapsed_ns = cycles_to_nanos(t1 - t0, freq);
                    histogram.record(elapsed_ns);
                    break;
                }
            } else {
                core::hint::spin_loop();
            }
        }

        // Drain the trailing OrderAccepted event
        loop {
            if outbound_consumer.try_pop().is_some() {
                break;
            } else {
                core::hint::spin_loop();
            }
        }
    }

    // Clean shutdown
    cmd_producer.try_push(EngineCommand::Shutdown).unwrap();
    engine_handle.join().unwrap();

    println!("\n=== Tick-to-Trade (T2T) Profile Results ({} Samples) ===", histogram.total_samples());
    println!("Min Latency:   {:>6} ns", histogram.min_ns());
    println!("Mean Latency:  {:>9.2} ns", histogram.mean_ns());
    println!("P50 (Median):  {:>6} ns", histogram.percentile(50.0));
    println!("P90:           {:>6} ns", histogram.percentile(90.0));
    println!("P99:           {:>6} ns", histogram.percentile(99.0));
    println!("P99.9:         {:>6} ns", histogram.percentile(99.9));
    println!("Max Latency:   {:>6} ns", histogram.max_ns());
}
