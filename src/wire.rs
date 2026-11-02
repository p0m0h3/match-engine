//! Binary wire protocol specifications for high-performance TCP transmission.

use crate::engine::{EngineCommand, EngineEvent};
use crate::types::{Order, OrderId, OrderType, Price, Quantity, Side, TimeInForce, Trade};

const _: () = assert!(
    cfg!(target_endian = "little"),
    "Wire protocol memory layout assumes a little-endian target architecture"
);
const _: () = assert!(std::mem::size_of::<WireCommandFrame>() == 40);
const _: () = assert!(std::mem::size_of::<WireEventFrame>() == 64);

/// Inbound binary frame (40 bytes fixed size).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireCommandFrame {
    pub order_id: u64,
    pub price: u64,
    pub quantity: u64,
    pub correlation_id: u64,
    pub msg_type: u8,      // 1 = Place, 2 = Cancel, 255 = Shutdown
    pub side: u8,          // 0 = Buy, 1 = Sell
    pub order_type: u8,    // 0 = Limit, 1 = Market
    pub time_in_force: u8, // 0 = GTC, 1 = IOC, 2 = FOK, 3 = PostOnly
    pub _padding: [u8; 4], // 8-byte natural alignment
}

impl WireCommandFrame {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub fn new_place_limit(
        order_id: u64,
        side: u8,
        price: u64,
        quantity: u64,
        correlation_id: u64,
    ) -> Self {
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

    pub fn new_modify(
        order_id: u64,
        new_price: u64,
        new_quantity: u64,
        correlation_id: u64,
    ) -> Self {
        Self {
            order_id,
            price: new_price,
            quantity: new_quantity,
            correlation_id,
            msg_type: 3,
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
            3 => Ok(EngineCommand::ModifyOrder {
                order_id: OrderId(self.order_id),
                new_price: if self.price > 0 {
                    Some(Price(self.price))
                } else {
                    None
                },
                new_quantity: Quantity(self.quantity),
            }),
            255 => Ok(EngineCommand::Shutdown),
            _ => Err("Unknown message type"),
        }
    }

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..8].copy_from_slice(&self.order_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.price.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.quantity.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.correlation_id.to_le_bytes());
        bytes[32] = self.msg_type;
        bytes[33] = self.side;
        bytes[34] = self.order_type;
        bytes[35] = self.time_in_force;
        bytes[36..40].copy_from_slice(&self._padding);
        bytes
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        // Safe: target architecture is little-endian (asserted at compile time),
        // and WireCommandFrame is #[repr(C)] with all 40 bytes explicitly initialized
        // as struct fields (no uninitialized padding bytes).
        unsafe { std::slice::from_raw_parts((self as *const Self) as *const u8, Self::SIZE) }
    }

    pub fn from_bytes(bytes: &[u8; Self::SIZE]) -> Self {
        Self {
            order_id: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            price: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            quantity: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            correlation_id: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            msg_type: bytes[32],
            side: bytes[33],
            order_type: bytes[34],
            time_in_force: bytes[35],
            _padding: [bytes[36], bytes[37], bytes[38], bytes[39]],
        }
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
            EngineEvent::OrderModified {
                order_id,
                new_price,
                new_quantity,
            } => Self {
                sequence_id: seq,
                timestamp_ns: ts_ns,
                order_id: order_id.0,
                maker_order_id: 0,
                price: new_price.0,
                quantity: new_quantity.0,
                leaves_quantity: new_quantity.0,
                event_type: 6, // 6 = OrderModified
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
            EngineEvent::L2Update(delta) => Self {
                sequence_id: seq,
                timestamp_ns: ts_ns,
                order_id: 0,
                maker_order_id: delta.side as u64,
                price: delta.price.0,
                quantity: delta.new_total_quantity.0,
                leaves_quantity: 0,
                event_type: 5, // 5 = L2 Update
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

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..8].copy_from_slice(&self.sequence_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.timestamp_ns.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.order_id.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.maker_order_id.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.price.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.quantity.to_le_bytes());
        bytes[48..56].copy_from_slice(&self.leaves_quantity.to_le_bytes());
        bytes[56] = self.event_type;
        bytes[57..64].copy_from_slice(&self._padding);
        bytes
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        // Safe: target architecture is little-endian (asserted at compile time),
        // and WireEventFrame is #[repr(C)] with all 64 bytes explicitly initialized
        // as struct fields (no uninitialized padding bytes).
        unsafe { std::slice::from_raw_parts((self as *const Self) as *const u8, Self::SIZE) }
    }

    pub fn from_bytes(bytes: &[u8; Self::SIZE]) -> Self {
        Self {
            sequence_id: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            timestamp_ns: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            order_id: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            maker_order_id: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            price: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            quantity: u64::from_le_bytes(bytes[40..48].try_into().unwrap()),
            leaves_quantity: u64::from_le_bytes(bytes[48..56].try_into().unwrap()),
            event_type: bytes[56],
            _padding: [bytes[57], bytes[58], bytes[59], bytes[60], bytes[61], bytes[62], bytes[63]],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_command_frame_roundtrip() {
        let frame = WireCommandFrame::new_place_limit(1001, 1, 55000, 250, 42);
        let bytes = frame.to_bytes();
        assert_eq!(bytes.len(), WireCommandFrame::SIZE);
        assert_eq!(frame.as_bytes(), &bytes);

        let decoded = WireCommandFrame::from_bytes(&bytes);
        assert_eq!(frame, decoded);
    }

    #[test]
    fn test_wire_event_frame_roundtrip() {
        let event = EngineEvent::TradeExecuted(Trade {
            maker_order_id: OrderId(101),
            taker_order_id: OrderId(102),
            price: Price(4500),
            quantity: Quantity(50),
            maker_leaves_quantity: Quantity(20),
            taker_leaves_quantity: Quantity(0),
        });
        let frame = WireEventFrame::from_engine_event(777, 1_600_000_000, &event);
        let bytes = frame.to_bytes();
        assert_eq!(bytes.len(), WireEventFrame::SIZE);
        assert_eq!(frame.as_bytes(), &bytes);

        let decoded = WireEventFrame::from_bytes(&bytes);
        assert_eq!(frame, decoded);
    }
}
