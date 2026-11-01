use match_engine::orderbook::{OrderBook, OrderPlacementResult};
use match_engine::types::{Order, OrderId, OrderType, Price, Quantity, Side, TimeInForce};

fn main() {
    println!("=== High-Performance Match Engine ===");

    let mut book = OrderBook::new();

    // 1. Seed resting liquidity (Maker orders)
    let ask1 = Order::new_limit(OrderId(1), Side::Sell, Price(100), Quantity(10));
    let ask2 = Order::new_limit(OrderId(2), Side::Sell, Price(100), Quantity(20));
    book.place_order(ask1);
    book.place_order(ask2);
    println!("Seeded Asks: 10 @ 100 (Order 1), 20 @ 100 (Order 2)");

    // 2. Taker order triggers partial fills
    let aggressive_buy = Order::new_limit(OrderId(3), Side::Buy, Price(100), Quantity(15));
    println!("Incoming Buy for 15 @ 100 (Order 3)...");

    if let OrderPlacementResult::Accepted { trades, .. } = book.place_order(aggressive_buy) {
        for trade in trades {
            println!(
                " -> Trade: Matched {} units @ price {}. Maker #{} (leaves: {}), Taker #{} (leaves: {})",
                trade.quantity.0,
                trade.price.0,
                trade.maker_order_id.0,
                trade.maker_leaves_quantity.0,
                trade.taker_order_id.0,
                trade.taker_leaves_quantity.0,
            );
        }
    }

    // 3. Post-Only test (rejection)
    let post_only = Order::new(
        OrderId(4),
        Side::Buy,
        OrderType::Limit,
        TimeInForce::PostOnly,
        Price(100),
        Quantity(5),
    );
    let result = book.place_order(post_only);
    println!("Post-Only order placement result: {:?}", result);

    println!("Current BBO: Best Bid: {:?}, Best Ask: {:?}", book.best_bid(), book.best_ask());
}
