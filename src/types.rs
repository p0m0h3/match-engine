//! Fundamental domain types and binary-aligned structures for the matching engine.

/// Unique 64-bit identifier for an order.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct OrderId(pub u64);

/// Discrete price point represented in tick units (e.g. $100.50 with 0.01 tick = 10050).
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Price(pub u64);

/// Order volume or matched size in atomic lot units.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Quantity(pub u64);

/// Trading side of an order.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

impl Side {
    #[inline(always)]
    pub fn opposite(self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

/// Price constraint specification.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderType {
    Limit = 0,
    Market = 1,
}

/// Lifetime and placement behavior instructions.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeInForce {
    /// Good 'Til Cancel: Aggressive portion matches; remainder rests on book.
    Gtc = 0,
    /// Immediate-Or-Cancel: Aggressive portion matches; remainder cancelled immediately.
    Ioc = 1,
    /// Fill-Or-Kill: Entire order must fill immediately, or order is cancelled with 0 fills.
    Fok = 2,
    /// Post-Only: Guaranteed liquidity maker; rejected if it would cross and take liquidity.
    PostOnly = 3,
}

/// Inbound command to submit an order.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Order {
    pub id: OrderId,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub price: Price,
    pub initial_quantity: Quantity,
    pub leaves_quantity: Quantity,
}

impl Order {
    #[inline]
    pub fn new_limit(id: OrderId, side: Side, price: Price, quantity: Quantity) -> Self {
        Self {
            id,
            side,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            price,
            initial_quantity: quantity,
            leaves_quantity: quantity,
        }
    }

    #[inline]
    pub fn new(
        id: OrderId,
        side: Side,
        order_type: OrderType,
        time_in_force: TimeInForce,
        price: Price,
        quantity: Quantity,
    ) -> Self {
        Self {
            id,
            side,
            order_type,
            time_in_force,
            price,
            initial_quantity: quantity,
            leaves_quantity: quantity,
        }
    }

    #[inline(always)]
    pub fn cum_quantity(&self) -> Quantity {
        Quantity(self.initial_quantity.0 - self.leaves_quantity.0)
    }

    #[inline(always)]
    pub fn is_filled(&self) -> bool {
        self.leaves_quantity.0 == 0
    }

    pub const SERIALIZED_SIZE: usize = 40;

    /// Serialize order into a 40-byte little-endian binary representation with explicit field alignment.
    pub fn to_bytes(&self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut buf = [0u8; Self::SERIALIZED_SIZE];
        buf[0..8].copy_from_slice(&self.id.0.to_le_bytes());
        buf[8] = self.side as u8;
        buf[9] = self.order_type as u8;
        buf[10] = self.time_in_force as u8;
        // bytes 11..16 are zeroed padding for 8-byte alignment
        buf[16..24].copy_from_slice(&self.price.0.to_le_bytes());
        buf[24..32].copy_from_slice(&self.initial_quantity.0.to_le_bytes());
        buf[32..40].copy_from_slice(&self.leaves_quantity.0.to_le_bytes());
        buf
    }

    /// Deserialize order from a 40-byte little-endian binary payload, validating enum variants.
    pub fn from_bytes(buf: &[u8; Self::SERIALIZED_SIZE]) -> Result<Self, &'static str> {
        let side = match buf[8] {
            0 => Side::Buy,
            1 => Side::Sell,
            _ => return Err("Invalid order side in binary payload"),
        };
        let order_type = match buf[9] {
            0 => OrderType::Limit,
            1 => OrderType::Market,
            _ => return Err("Invalid order type in binary payload"),
        };
        let time_in_force = match buf[10] {
            0 => TimeInForce::Gtc,
            1 => TimeInForce::Ioc,
            2 => TimeInForce::Fok,
            3 => TimeInForce::PostOnly,
            _ => return Err("Invalid time in force in binary payload"),
        };

        Ok(Self {
            id: OrderId(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
            side,
            order_type,
            time_in_force,
            price: Price(u64::from_le_bytes(buf[16..24].try_into().unwrap())),
            initial_quantity: Quantity(u64::from_le_bytes(buf[24..32].try_into().unwrap())),
            leaves_quantity: Quantity(u64::from_le_bytes(buf[32..40].try_into().unwrap())),
        })
    }
}

const _: () = assert!(std::mem::size_of::<Order>() == Order::SERIALIZED_SIZE);

/// Execution trade record emitted when orders match.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trade {
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub price: Price,
    pub quantity: Quantity,
    pub maker_leaves_quantity: Quantity,
    pub taker_leaves_quantity: Quantity,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_serialization_roundtrip() {
        let order = Order::new(
            OrderId(12345),
            Side::Sell,
            OrderType::Limit,
            TimeInForce::PostOnly,
            Price(98765),
            Quantity(500),
        );

        let bytes = order.to_bytes();
        assert_eq!(bytes.len(), Order::SERIALIZED_SIZE);

        let decoded = Order::from_bytes(&bytes).expect("Valid order bytes should decode");
        assert_eq!(order, decoded);
    }

    #[test]
    fn test_order_deserialization_invalid_enums() {
        let mut bytes = [0u8; Order::SERIALIZED_SIZE];
        bytes[8] = 99; // invalid Side
        assert!(Order::from_bytes(&bytes).is_err());

        bytes[8] = 0; // valid Side
        bytes[9] = 99; // invalid OrderType
        assert!(Order::from_bytes(&bytes).is_err());

        bytes[9] = 0; // valid OrderType
        bytes[10] = 99; // invalid TimeInForce
        assert!(Order::from_bytes(&bytes).is_err());
    }
}
