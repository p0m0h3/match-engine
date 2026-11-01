use criterion::{black_box, criterion_group, criterion_main, Criterion};
use match_engine::orderbook::OrderBook;
use match_engine::types::{Order, OrderId, Price, Quantity, Side};

fn bench_passive_order_placement(c: &mut Criterion) {
    c.bench_function("passive_limit_order_placement", |b| {
        b.iter_batched(
            || OrderBook::new(),
            |mut book| {
                for i in 1..=1_000 {
                    let order = Order::new_limit(
                        OrderId(i),
                        Side::Buy,
                        Price(1000 + (i % 50)),
                        Quantity(10),
                    );
                    black_box(book.place_order(black_box(order)));
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_aggressive_crossing_matches(c: &mut Criterion) {
    c.bench_function("aggressive_crossing_matches", |b| {
        b.iter_batched(
            || {
                let mut book = OrderBook::new();
                // Pre-seed 1,000 asks at 100 levels
                for i in 1..=1_000 {
                    let ask = Order::new_limit(
                        OrderId(i),
                        Side::Sell,
                        Price(1000 + (i % 100)),
                        Quantity(10),
                    );
                    book.place_order(ask);
                }
                book
            },
            |mut book| {
                // Aggressive buy matching against existing asks
                for i in 1_001..=1_200 {
                    let aggressive_buy = Order::new_limit(
                        OrderId(i),
                        Side::Buy,
                        Price(2000), // Crosses everything
                        Quantity(5),
                    );
                    black_box(book.place_order(black_box(aggressive_buy)));
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_order_cancellation(c: &mut Criterion) {
    c.bench_function("cancel_order_from_queue", |b| {
        b.iter_batched(
            || {
                let mut book = OrderBook::new();
                // Fill book with 2,000 orders across few levels (deep queues)
                for i in 1..=2_000 {
                    let order = Order::new_limit(
                        OrderId(i),
                        Side::Buy,
                        Price(1000 + (i % 10)),
                        Quantity(10),
                    );
                    book.place_order(order);
                }
                book
            },
            |mut book| {
                // Cancel 500 orders (demonstrating queue scan penalty)
                for i in 1..=500 {
                    black_box(book.cancel_order(black_box(OrderId(i))));
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_passive_order_placement,
    bench_aggressive_crossing_matches,
    bench_order_cancellation
);
criterion_main!(benches);
