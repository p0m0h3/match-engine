//! Idiomatic, strongly-typed Rust Client SDK for the high-performance Match Engine.
//!
//! Provides decoupled `InboundClient` (for OMS / order routing) and `EventSubscriber`
//! (for market data / trade feeds), as well as a composite `MatchEngineClient`.

use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};

use crate::types::{OrderId, Price, Quantity, Side, TimeInForce};
use crate::wire::{WireCommandFrame, WireEventFrame};

/// Decoded high-level client event emitted by the matching engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientEvent {
    OrderAccepted {
        sequence_id: u64,
        timestamp_ns: u64,
        order_id: OrderId,
        resting: bool,
    },
    TradeExecuted {
        sequence_id: u64,
        timestamp_ns: u64,
        taker_order_id: OrderId,
        maker_order_id: OrderId,
        price: Price,
        quantity: Quantity,
        maker_leaves_quantity: Quantity,
    },
    OrderCancelled {
        sequence_id: u64,
        timestamp_ns: u64,
        order_id: OrderId,
    },
    OrderModified {
        sequence_id: u64,
        timestamp_ns: u64,
        order_id: OrderId,
        new_price: Price,
        new_quantity: Quantity,
    },
    OrderRejected {
        sequence_id: u64,
        timestamp_ns: u64,
        order_id: OrderId,
    },
    EngineShutdown {
        sequence_id: u64,
        timestamp_ns: u64,
    },
    Unknown {
        sequence_id: u64,
        event_type: u8,
    },
}

/// Strongly-typed client for submitting commands to Inbound Port 9001 (OMS role).
pub struct InboundClient {
    writer: BufWriter<TcpStream>,
}

impl InboundClient {
    pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

    /// Connect to the engine's inbound TCP gateway using default 64 KB buffering.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Self::connect_with_buffer_size(addr, Self::DEFAULT_BUFFER_SIZE)
    }

    /// Connect with a custom buffer size for tuning network batching.
    pub fn connect_with_buffer_size<A: ToSocketAddrs>(
        addr: A,
        buffer_size: usize,
    ) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        Ok(Self {
            writer: BufWriter::with_capacity(buffer_size, stream),
        })
    }

    /// Submit a generic limit order with specified Time-In-Force.
    pub fn place_limit(
        &mut self,
        order_id: OrderId,
        side: Side,
        price: Price,
        quantity: Quantity,
        time_in_force: TimeInForce,
        correlation_id: u64,
    ) -> io::Result<()> {
        let frame = WireCommandFrame {
            order_id: order_id.0,
            price: price.0,
            quantity: quantity.0,
            correlation_id,
            msg_type: 1,
            side: match side {
                Side::Buy => 0,
                Side::Sell => 1,
            },
            order_type: 0, // Limit
            time_in_force: match time_in_force {
                TimeInForce::Gtc => 0,
                TimeInForce::Ioc => 1,
                TimeInForce::Fok => 2,
                TimeInForce::PostOnly => 3,
            },
            _padding: [0; 4],
        };
        self.writer.write_all(frame.as_bytes())
    }

    /// Helper for placing a standard Good 'Til Cancel (GTC) limit order.
    pub fn place_gtc_limit(
        &mut self,
        order_id: OrderId,
        side: Side,
        price: Price,
        quantity: Quantity,
        correlation_id: u64,
    ) -> io::Result<()> {
        self.place_limit(
            order_id,
            side,
            price,
            quantity,
            TimeInForce::Gtc,
            correlation_id,
        )
    }

    /// Helper for placing a Post-Only (Maker-Only) limit order.
    pub fn place_post_only(
        &mut self,
        order_id: OrderId,
        side: Side,
        price: Price,
        quantity: Quantity,
        correlation_id: u64,
    ) -> io::Result<()> {
        self.place_limit(
            order_id,
            side,
            price,
            quantity,
            TimeInForce::PostOnly,
            correlation_id,
        )
    }

    /// Submit an Immediate-Or-Cancel (IOC) Market Order.
    pub fn place_market(
        &mut self,
        order_id: OrderId,
        side: Side,
        quantity: Quantity,
        correlation_id: u64,
    ) -> io::Result<()> {
        let frame = WireCommandFrame {
            order_id: order_id.0,
            price: 0,
            quantity: quantity.0,
            correlation_id,
            msg_type: 1,
            side: match side {
                Side::Buy => 0,
                Side::Sell => 1,
            },
            order_type: 1,    // Market
            time_in_force: 1, // IOC
            _padding: [0; 4],
        };
        self.writer.write_all(frame.as_bytes())
    }

    /// Request cancellation of an active resting order.
    pub fn cancel_order(&mut self, order_id: OrderId, correlation_id: u64) -> io::Result<()> {
        let frame = WireCommandFrame::new_cancel(order_id.0, correlation_id);
        self.writer.write_all(frame.as_bytes())
    }

    /// Request atomic Cancel-Replace / Order Amendment.
    pub fn modify_order(
        &mut self,
        order_id: OrderId,
        new_price: Option<Price>,
        new_quantity: Quantity,
        correlation_id: u64,
    ) -> io::Result<()> {
        let frame = WireCommandFrame::new_modify(
            order_id.0,
            new_price.map(|p| p.0).unwrap_or(0),
            new_quantity.0,
            correlation_id,
        );
        self.writer.write_all(frame.as_bytes())
    }

    /// Request engine shutdown.
    pub fn shutdown(&mut self) -> io::Result<()> {
        let frame = WireCommandFrame::new_shutdown();
        self.writer.write_all(frame.as_bytes())?;
        self.writer.flush()
    }

    /// Flush all buffered frames to the network socket immediately.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Strongly-typed subscriber reading the outbound event broadcast stream (Port 9002).
pub struct EventSubscriber {
    reader: BufReader<TcpStream>,
    last_sequence_id: u64,
    read_buf: [u8; WireEventFrame::SIZE],
}

impl EventSubscriber {
    pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

    /// Connect to the engine's outbound broadcast port using default 64 KB buffering.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Self::connect_with_buffer_size(addr, Self::DEFAULT_BUFFER_SIZE)
    }

    /// Connect with a custom buffer size.
    pub fn connect_with_buffer_size<A: ToSocketAddrs>(
        addr: A,
        buffer_size: usize,
    ) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        stream.set_nonblocking(false)?;
        Ok(Self {
            reader: BufReader::with_capacity(buffer_size, stream),
            last_sequence_id: 0,
            read_buf: [0u8; WireEventFrame::SIZE],
        })
    }

    /// Monotonically increasing sequence ID of the last event read.
    pub fn last_sequence_id(&self) -> u64 {
        self.last_sequence_id
    }

    /// Read next raw binary event frame from the network stream.
    pub fn read_frame(&mut self) -> io::Result<WireEventFrame> {
        self.reader.read_exact(&mut self.read_buf)?;
        let frame = WireEventFrame::from_bytes(&self.read_buf);
        self.last_sequence_id = frame.sequence_id;
        Ok(frame)
    }

    /// Read next decoded domain event.
    pub fn read_event(&mut self) -> io::Result<ClientEvent> {
        let frame = self.read_frame()?;
        Ok(match frame.event_type {
            1 => ClientEvent::OrderAccepted {
                sequence_id: frame.sequence_id,
                timestamp_ns: frame.timestamp_ns,
                order_id: OrderId(frame.order_id),
                resting: frame.leaves_quantity > 0,
            },
            2 => ClientEvent::TradeExecuted {
                sequence_id: frame.sequence_id,
                timestamp_ns: frame.timestamp_ns,
                taker_order_id: OrderId(frame.order_id),
                maker_order_id: OrderId(frame.maker_order_id),
                price: Price(frame.price),
                quantity: Quantity(frame.quantity),
                maker_leaves_quantity: Quantity(frame.leaves_quantity),
            },
            3 => ClientEvent::OrderCancelled {
                sequence_id: frame.sequence_id,
                timestamp_ns: frame.timestamp_ns,
                order_id: OrderId(frame.order_id),
            },
            4 => ClientEvent::OrderRejected {
                sequence_id: frame.sequence_id,
                timestamp_ns: frame.timestamp_ns,
                order_id: OrderId(frame.order_id),
            },
            6 => ClientEvent::OrderModified {
                sequence_id: frame.sequence_id,
                timestamp_ns: frame.timestamp_ns,
                order_id: OrderId(frame.order_id),
                new_price: Price(frame.price),
                new_quantity: Quantity(frame.quantity),
            },
            255 => ClientEvent::EngineShutdown {
                sequence_id: frame.sequence_id,
                timestamp_ns: frame.timestamp_ns,
            },
            other => ClientEvent::Unknown {
                sequence_id: frame.sequence_id,
                event_type: other,
            },
        })
    }
}

impl Iterator for EventSubscriber {
    type Item = io::Result<ClientEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_event() {
            Ok(ClientEvent::EngineShutdown { .. }) => None,
            Ok(ev) => Some(Ok(ev)),
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Unified composite client bundle managing both Inbound commands and Outbound events.
pub struct MatchEngineClient {
    pub inbound: InboundClient,
    pub subscriber: EventSubscriber,
}

impl MatchEngineClient {
    pub fn connect<A: ToSocketAddrs + Copy>(inbound_addr: A, outbound_addr: A) -> io::Result<Self> {
        let subscriber = EventSubscriber::connect(outbound_addr)?;
        let inbound = InboundClient::connect(inbound_addr)?;
        Ok(Self {
            inbound,
            subscriber,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::run_matching_engine_loop;
    use crate::gateway::{start_inbound_gateway, start_outbound_broadcaster};
    use crate::ring_buffer::spsc_channel;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_client_library_end_to_end() {
        let in_addr = "127.0.0.1:19003";
        let out_addr = "127.0.0.1:19004";

        const CAPACITY: usize = 1024;
        let (inbound_producer, engine_cmd_consumer) = spsc_channel::<_, CAPACITY>();
        let (engine_event_producer, outbound_consumer) = spsc_channel::<_, CAPACITY>();
        let running = Arc::new(AtomicBool::new(true));

        let in_handle = start_inbound_gateway(in_addr, inbound_producer, Arc::clone(&running));
        let out_handle =
            start_outbound_broadcaster(out_addr, outbound_consumer, Arc::clone(&running));

        let r_engine = Arc::clone(&running);
        let engine_handle = thread::spawn(move || {
            run_matching_engine_loop(engine_cmd_consumer, engine_event_producer, r_engine);
        });

        thread::sleep(Duration::from_millis(50));

        // Connect client using unified MatchEngineClient
        let mut client =
            MatchEngineClient::connect(in_addr, out_addr).expect("Client failed to connect");

        // 1. Submit Resting Maker Sell Order
        client
            .inbound
            .place_gtc_limit(OrderId(10), Side::Sell, Price(1050), Quantity(100), 1)
            .unwrap();
        client.inbound.flush().unwrap();

        // Verify Event
        let ev1 = client.subscriber.read_event().unwrap();
        assert_eq!(
            ev1,
            ClientEvent::OrderAccepted {
                sequence_id: 1,
                timestamp_ns: ev1_timestamp(&ev1),
                order_id: OrderId(10),
                resting: true,
            }
        );

        // 2. Modify Order Quantity down to 80 units
        client
            .inbound
            .modify_order(OrderId(10), None, Quantity(80), 2)
            .unwrap();
        client.inbound.flush().unwrap();

        let ev2 = client.subscriber.read_event().unwrap();
        assert_eq!(
            ev2,
            ClientEvent::OrderModified {
                sequence_id: 2,
                timestamp_ns: ev1_timestamp(&ev2),
                order_id: OrderId(10),
                new_price: Price(1050),
                new_quantity: Quantity(80),
            }
        );

        // 3. Submit Crossing Aggressive Buy Order
        client
            .inbound
            .place_market(OrderId(11), Side::Buy, Quantity(30), 3)
            .unwrap();
        client.inbound.flush().unwrap();

        // Should receive TradeExecuted then OrderAccepted
        let ev3 = client.subscriber.read_event().unwrap();
        assert_eq!(
            ev3,
            ClientEvent::TradeExecuted {
                sequence_id: 3,
                timestamp_ns: ev1_timestamp(&ev3),
                taker_order_id: OrderId(11),
                maker_order_id: OrderId(10),
                price: Price(1050),
                quantity: Quantity(30),
                maker_leaves_quantity: Quantity(50),
            }
        );

        let ev4 = client.subscriber.read_event().unwrap();
        assert_eq!(
            ev4,
            ClientEvent::OrderAccepted {
                sequence_id: 4,
                timestamp_ns: ev1_timestamp(&ev4),
                order_id: OrderId(11),
                resting: false,
            }
        );

        // 4. Cancel Remaining Resting Order
        client.inbound.cancel_order(OrderId(10), 4).unwrap();
        client.inbound.flush().unwrap();

        let ev5 = client.subscriber.read_event().unwrap();
        assert_eq!(
            ev5,
            ClientEvent::OrderCancelled {
                sequence_id: 5,
                timestamp_ns: ev1_timestamp(&ev5),
                order_id: OrderId(10),
            }
        );

        // 5. Clean Shutdown
        client.inbound.shutdown().unwrap();
        let ev6 = client.subscriber.read_event().unwrap();
        assert!(matches!(ev6, ClientEvent::EngineShutdown { .. }));

        running.store(false, Ordering::Relaxed);
        engine_handle.join().unwrap();
        in_handle.join().unwrap();
        out_handle.join().unwrap();
    }

    fn ev1_timestamp(ev: &ClientEvent) -> u64 {
        match ev {
            ClientEvent::OrderAccepted { timestamp_ns, .. } => *timestamp_ns,
            ClientEvent::TradeExecuted { timestamp_ns, .. } => *timestamp_ns,
            ClientEvent::OrderCancelled { timestamp_ns, .. } => *timestamp_ns,
            ClientEvent::OrderModified { timestamp_ns, .. } => *timestamp_ns,
            ClientEvent::OrderRejected { timestamp_ns, .. } => *timestamp_ns,
            ClientEvent::EngineShutdown { timestamp_ns, .. } => *timestamp_ns,
            ClientEvent::Unknown { .. } => 0,
        }
    }
}
