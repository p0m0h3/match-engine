//! Engine runner driving the single-writer event loop via lock-free SPSC channels.

use crate::orderbook::{OrderBook, OrderPlacementResult};
use crate::ring_buffer::{Consumer, Producer};
use crate::types::{Order, OrderId, Price, Quantity, Trade};

/// Inbound command processed by the matching engine loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineCommand {
    PlaceOrder(Order),
    CancelOrder(OrderId),
    ModifyOrder {
        order_id: OrderId,
        new_price: Option<Price>,
        new_quantity: Quantity,
    },
    Shutdown,
}

/// Outbound event emitted by the matching engine loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    OrderAccepted {
        order_id: OrderId,
        resting: bool,
    },
    OrderModified {
        order_id: OrderId,
        new_price: Price,
        new_quantity: Quantity,
    },
    TradeExecuted(Trade),
    OrderCancelled {
        order_id: OrderId,
    },
    OrderRejected {
        order_id: OrderId,
        reason: &'static str,
    },
    L2Update(crate::market_data::L2DeltaUpdate),
    EngineShutdown,
}

/// Runs the single-writer matching engine event loop on the current pinned thread.
pub fn run_matching_engine_loop<const IN_SIZE: usize, const OUT_SIZE: usize>(
    mut cmd_consumer: Consumer<EngineCommand, IN_SIZE>,
    mut event_producer: Producer<EngineEvent, OUT_SIZE>,
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    let mut book = OrderBook::new();
    let mut dedup_filter = crate::dedup::SlidingDedupFilter::with_capacity(65_536);

    while running.load(std::sync::atomic::Ordering::Relaxed) {
        // Poll non-blocking from inbound lock-free ring buffer
        if let Some(cmd) = cmd_consumer.try_pop() {
            match cmd {
                EngineCommand::PlaceOrder(order) => {
                    let order_id = order.id;

                    // Idempotency check: drop historical duplicates that already finished
                    if dedup_filter.contains(order_id) {
                        push_event_spinning(
                            &mut event_producer,
                            EngineEvent::OrderRejected {
                                order_id,
                                reason: "Duplicate OrderId: order has already executed",
                            },
                        );
                        continue;
                    }

                    match book.place_order(order) {
                        OrderPlacementResult::Accepted {
                            trades,
                            resting_order_id,
                        } => {
                            // If order did not rest, record to dedup filter so retries are rejected
                            if resting_order_id.is_none() {
                                dedup_filter.record(order_id);
                            }

                            // Emit trade events first
                            for trade in trades {
                                push_event_spinning(
                                    &mut event_producer,
                                    EngineEvent::TradeExecuted(trade),
                                );
                            }
                            // Emit accepted event
                            push_event_spinning(
                                &mut event_producer,
                                EngineEvent::OrderAccepted {
                                    order_id,
                                    resting: resting_order_id.is_some(),
                                },
                            );
                        }
                        OrderPlacementResult::Rejected { reason } => {
                            push_event_spinning(
                                &mut event_producer,
                                EngineEvent::OrderRejected { order_id, reason },
                            );
                        }
                    }
                }
                EngineCommand::CancelOrder(order_id) => {
                    if let Some(_cancelled) = book.cancel_order(order_id) {
                        dedup_filter.record(order_id);
                        push_event_spinning(
                            &mut event_producer,
                            EngineEvent::OrderCancelled { order_id },
                        );
                    } else {
                        push_event_spinning(
                            &mut event_producer,
                            EngineEvent::OrderRejected {
                                order_id,
                                reason: "Order not found for cancellation",
                            },
                        );
                    }
                }
                EngineCommand::ModifyOrder {
                    order_id,
                    new_price,
                    new_quantity,
                } => match book.modify_order(order_id, new_price, new_quantity) {
                    Ok((modified_order, trades)) => {
                        for trade in trades {
                            push_event_spinning(
                                &mut event_producer,
                                EngineEvent::TradeExecuted(trade),
                            );
                        }
                        push_event_spinning(
                            &mut event_producer,
                            EngineEvent::OrderModified {
                                order_id,
                                new_price: modified_order.price,
                                new_quantity: modified_order.leaves_quantity,
                            },
                        );
                    }
                    Err(reason) => {
                        push_event_spinning(
                            &mut event_producer,
                            EngineEvent::OrderRejected { order_id, reason },
                        );
                    }
                },
                EngineCommand::Shutdown => {
                    push_event_spinning(&mut event_producer, EngineEvent::EngineShutdown);
                    break;
                }
            }
        } else {
            // Hot spin: Lowest possible wake-up latency for pinned CPU core
            core::hint::spin_loop();
        }
    }
}

#[inline(always)]
fn push_event_spinning<const N: usize>(
    producer: &mut Producer<EngineEvent, N>,
    mut event: EngineEvent,
) {
    loop {
        match producer.try_push(event) {
            Ok(()) => break,
            Err(e) => {
                event = e;
                core::hint::spin_loop();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ring_buffer::spsc_channel;
    use crate::types::{Price, Quantity, Side};
    use std::thread;

    #[test]
    fn test_end_to_end_matching_loop() {
        const QUEUE_CAPACITY: usize = 128;

        let (mut cmd_producer, cmd_consumer) = spsc_channel::<EngineCommand, QUEUE_CAPACITY>();
        let (event_producer, mut event_consumer) = spsc_channel::<EngineEvent, QUEUE_CAPACITY>();

        let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let r_engine = std::sync::Arc::clone(&running);

        // Spawn the dedicated matching thread
        let engine_handle = thread::spawn(move || {
            run_matching_engine_loop(cmd_consumer, event_producer, r_engine);
        });

        // Seed resting ask
        cmd_producer
            .try_push(EngineCommand::PlaceOrder(Order::new_limit(
                OrderId(1),
                Side::Sell,
                Price(100),
                Quantity(10),
            )))
            .unwrap();

        // Place crossing buy
        cmd_producer
            .try_push(EngineCommand::PlaceOrder(Order::new_limit(
                OrderId(2),
                Side::Buy,
                Price(100),
                Quantity(10),
            )))
            .unwrap();

        // Send shutdown
        cmd_producer.try_push(EngineCommand::Shutdown).unwrap();

        // Read outbound events from consumer
        let mut events = Vec::new();
        loop {
            if let Some(event) = event_consumer.try_pop() {
                let is_shutdown = matches!(event, EngineEvent::EngineShutdown);
                events.push(event);
                if is_shutdown {
                    break;
                }
            } else {
                core::hint::spin_loop();
            }
        }

        engine_handle.join().unwrap();

        // Verify order sequence:
        // 1. OrderAccepted (Order 1, resting)
        // 2. TradeExecuted (Trade between 1 and 2)
        // 3. OrderAccepted (Order 2, not resting)
        // 4. EngineShutdown
        assert_eq!(events.len(), 4);
        assert!(matches!(
            events[0],
            EngineEvent::OrderAccepted {
                order_id: OrderId(1),
                resting: true
            }
        ));
        assert!(matches!(events[1], EngineEvent::TradeExecuted(_)));
        assert!(matches!(
            events[2],
            EngineEvent::OrderAccepted {
                order_id: OrderId(2),
                resting: false
            }
        ));
        assert!(matches!(events[3], EngineEvent::EngineShutdown));
    }
}
