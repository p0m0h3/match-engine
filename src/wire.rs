//! Binary wire protocol specifications for high-performance TCP transmission.

use crate::engine::{EngineCommand, EngineEvent};
use crate::types::{Order, OrderId, OrderType, Price, Quantity, Side, TimeInForce, Trade};

/// Inbound binary frame (40 bytes fixed size).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireCommandFrame {
    pub order_id: u64,
    pub price: u64,
    pub quantity: u64,
    pub correlation_id: u64,
    pub msg_type: u8,       // 1 = Place, 2 = Cancel, 255 = Shutdown
    pub side: u8,           // 0 = Buy, 1 = Sell
    pub order_type: u8,     // 0 = Limit, 1 = Market
    pub time_in_force: u8,  // 0 = GTC, 1 = IOC, 2 = FOK, 3 = PostOnly
    pub _padding: [u8; 4],  // 8-byte natural alignment
}

impl WireCommandFrame {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub fn new_place_limit(order_id: u64, side: u8, price: u64, quantity: u64, correlation_id: u64) -> Self {
        Self {
            order_id,
            price,
            quantity,
            correlation_id,
            msg_type: 1,
            side,
            order_type: 0,
            time_in_force: 0,
            _padding: [0; 4],
        }
    }

    pub fn new_cancel(order_id: u64, correlation_id: u64) -> Self {
        Self {
            order_id,
            price: 0,
            quantity: 0,
            correlation_id,
            msg_type: 2,
            side: 0,
            order_type: 0,
            time_in_force: 0,
            _padding: [0; 4],
        }
    }

    pub fn new_shutdown() -> Self {
        Self {
            order_id: 0,
            price: 0,
            quantity: 0,
            correlation_id: 0,
            msg_type: 255,
            side: 0,
            order_type: 0,
            time_in_force: 0,
            _padding: [0; 4],
        }
    }

    pub fn to_engine_command(&self) -> Result<EngineCommand, &'static str> {
        match self.msg_type {
            1 => {
                let side = match self.side {
                    0 => Side::Buy,
                    1 => Side::Sell,
                    _ => return Err("Invalid side"),
                };
                let order_type = match self.order_type {
                    0 => OrderType::Limit,
                    1 => OrderType::Market,
                    _ => return Err("Invalid order type"),
                };
                let time_in_force = match self.time_in_force {
                    0 => TimeInForce::Gtc,
                    1 => TimeInForce::Ioc,
                    2 => TimeInForce::Fok,
                    3 => TimeInForce::PostOnly,
                    _ => return Err("Invalid time in force"),
                };

                Ok(EngineCommand::PlaceOrder(Order::new(
                    OrderId(self.order_id),
                    side,
                    order_type,
                    time_in_force,
                    Price(self.price),
                    Quantity(self.quantity),
                )))
            }
            2 => Ok(EngineCommand::CancelOrder(OrderId(self.order_id))),
            255 => Ok(EngineCommand::Shutdown),
            _ => Err("Unknown message type"),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                Self::SIZE,
            )
        }
    }

    pub fn from_bytes(bytes: &[u8; Self::SIZE]) -> Self {
        unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const Self) }
    }
}

/// Outbound binary event frame (56 bytes fixed size).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireEventFrame {
    pub sequence_id: u64,
    pub timestamp_ns: u64,
    pub order_id: u64,
    pub maker_order_id: u64,
    pub price: u64,
    pub quantity: u64,
    pub leaves_quantity: u64,
    pub event_type: u8, // 1 = Accepted, 2 = Trade, 3 = Cancelled, 4 = Rejected, 255 = Shutdown
    pub _padding: [u8; 7], // 8-byte natural alignment
}

impl WireEventFrame {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub fn from_engine_event(seq: u64, ts_ns: u64, event: &EngineEvent) -> Self {
        match event {
            EngineEvent::OrderAccepted { order_id, resting } => Self {
                sequence_id: seq,
                timestamp_ns: ts_ns,
                order_id: order_id.0,
                maker_order_id: 0,
                price: 0,
                quantity: 0,
                leaves_quantity: if *resting { 1 } else { 0 },
                event_type: 1,
                _padding: [0; 7],
            },
            EngineEvent::TradeExecuted(Trade {
                maker_order_id,
                taker_order_id,
                price,
                quantity,
                maker_leaves_quantity,
                taker_leaves_quantity: _,
            }) => Self {
                sequence_id: seq,
                timestamp_ns: ts_ns,
                order_id: taker_order_id.0,
                maker_order_id: maker_order_id.0,
                price: price.0,
                quantity: quantity.0,
                leaves_quantity: maker_leaves_quantity.0,
                event_type: 2,
                _padding: [0; 7],
            },
            EngineEvent::OrderCancelled { order_id } => Self {
                sequence_id: seq,
                timestamp_ns: ts_ns,
                order_id: order_id.0,
                maker_order_id: 0,
                price: 0,
                quantity: 0,
                leaves_quantity: 0,
                event_type: 3,
                _padding: [0; 7],
            },
            EngineEvent::OrderRejected { order_id, .. } => Self {
                sequence_id: seq,
                timestamp_ns: ts_ns,
                order_id: order_id.0,
                maker_order_id: 0,
                price: 0,
                quantity: 0,
                leaves_quantity: 0,
                event_type: 4,
                _padding: [0; 7],
            },
            EngineEvent::EngineShutdown => Self {
                sequence_id: seq,
                timestamp_ns: ts_ns,
                order_id: 0,
                maker_order_id: 0,
                price: 0,
                quantity: 0,
                leaves_quantity: 0,
                event_type: 255,
                _padding: [0; 7],
            },
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                Self::SIZE,
            )
        }
    }

    pub fn from_bytes(bytes: &[u8; Self::SIZE]) -> Self {
        unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const Self) }
    }
}
