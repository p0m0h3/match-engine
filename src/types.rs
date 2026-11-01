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
}

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
