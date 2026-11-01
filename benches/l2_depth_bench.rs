use criterion::{Criterion, black_box, criterion_group, criterion_main};
use match_engine::orderbook::OrderBook;
use match_engine::types::{Order, OrderId, Price, Quantity, Side};

fn bench_l2_snapshot_generation(c: &mut Criterion) {
    let mut book = OrderBook::new();

    // Populate book with 1,000 orders across 100 price levels
    for i in 1..=500 {
        book.place_order(Order::new_limit(
            OrderId(i),
            Side::Buy,
            Price(1000 - (i % 50)),
            Quantity(10),
        ));
    }
    for i in 501..=1000 {
        book.place_order(Order::new_limit(
            OrderId(i),
            Side::Sell,
            Price(1050 + (i % 50)),
            Quantity(10),
        ));
    }

    c.bench_function("l2_snapshot_top_20_levels", |b| {
        b.iter(|| {
            black_box(book.get_l2_snapshot::<20>());
        });
    });
}

criterion_group!(benches, bench_l2_snapshot_generation);
criterion_main!(benches);
