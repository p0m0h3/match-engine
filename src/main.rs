use match_engine::orderbook::OrderBook;
use match_engine::types::{Order, OrderId, Price, Quantity, Side};

fn main() {
    println!("Starting high-performance match engine...");

    let mut book = OrderBook::new();

    // Seed passive resting orders
    let resting_ask = Order::new(OrderId(1), Side::Sell, Price(100), Quantity(50));
    println!("Placing passive ask: {:?}", resting_ask);
    book.place_limit_order(resting_ask);

    let resting_bid = Order::new(OrderId(2), Side::Buy, Price(95), Quantity(40));
    println!("Placing passive bid: {:?}", resting_bid);
    book.place_limit_order(resting_bid);

    println!(
        "Current BBO -> Best Bid: {:?}, Best Ask: {:?}",
        book.best_bid(),
        book.best_ask()
    );

    // Place an aggressive crossing order
    let aggressive_buy = Order::new(OrderId(3), Side::Buy, Price(102), Quantity(25));
    println!("Placing aggressive crossing buy: {:?}", aggressive_buy);
    let trades = book.place_limit_order(aggressive_buy);

    println!("Trades generated ({}):", trades.len());
    for trade in trades {
        println!(
            " -> Matched! Maker Order #{}, Taker Order #{}: {} units @ price {}",
            trade.maker_order_id.0, trade.taker_order_id.0, trade.quantity.0, trade.price.0
        );
    }

    println!(
        "Updated BBO -> Best Bid: {:?}, Best Ask: {:?}",
        book.best_bid(),
        book.best_ask()
    );

    println!("Match engine executed cleanly.");
}
