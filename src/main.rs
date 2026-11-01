use match_engine::engine::{run_matching_engine_loop, EngineCommand, EngineEvent};
use match_engine::ring_buffer::spsc_channel;
use match_engine::types::{Order, OrderId, Price, Quantity, Side};
use std::thread;

fn main() {
    println!("=== High-Performance Lock-Free Match Engine ===");

    const CAPACITY: usize = 1024;
    let (mut cmd_producer, cmd_consumer) = spsc_channel::<EngineCommand, CAPACITY>();
    let (event_producer, mut event_consumer) = spsc_channel::<EngineEvent, CAPACITY>();

    // Spawn dedicated matching thread
    let engine_thread = thread::spawn(move || {
        println!("[Engine Thread] Started single-writer event loop.");
        run_matching_engine_loop(cmd_consumer, event_producer);
        println!("[Engine Thread] Loop terminated.");
    });

    // Gateway thread: Send orders
    println!("[Gateway Thread] Sending resting asks...");
    cmd_producer
        .try_push(EngineCommand::PlaceOrder(Order::new_limit(
            OrderId(1),
            Side::Sell,
            Price(100),
            Quantity(50),
        )))
        .unwrap();

    cmd_producer
        .try_push(EngineCommand::PlaceOrder(Order::new_limit(
            OrderId(2),
            Side::Buy,
            Price(90),
            Quantity(30),
        )))
        .unwrap();

    println!("[Gateway Thread] Sending crossing aggressive buy...");
    cmd_producer
        .try_push(EngineCommand::PlaceOrder(Order::new_limit(
            OrderId(3),
            Side::Buy,
            Price(100),
            Quantity(25),
        )))
        .unwrap();

    cmd_producer.try_push(EngineCommand::Shutdown).unwrap();

    // Event listener: Read events from the engine
    println!("[Listener Thread] Consuming output event stream:");
    loop {
        if let Some(event) = event_consumer.try_pop() {
            match event {
                EngineEvent::OrderAccepted { order_id, resting } => {
                    println!(" -> Event: OrderAccepted(id={}, resting={})", order_id.0, resting);
                }
                EngineEvent::TradeExecuted(trade) => {
                    println!(
                        " -> Event: TradeExecuted(maker={}, taker={}, qty={}, price={})",
                        trade.maker_order_id.0, trade.taker_order_id.0, trade.quantity.0, trade.price.0
                    );
                }
                EngineEvent::OrderCancelled { order_id } => {
                    println!(" -> Event: OrderCancelled(id={})", order_id.0);
                }
                EngineEvent::OrderRejected { order_id, reason } => {
                    println!(" -> Event: OrderRejected(id={}, reason={})", order_id.0, reason);
                }
                EngineEvent::EngineShutdown => {
                    println!(" -> Event: EngineShutdown received.");
                    break;
                }
            }
        } else {
            core::hint::spin_loop();
        }
    }

    engine_thread.join().unwrap();
    println!("=== Match Engine finished cleanly ===");
}
