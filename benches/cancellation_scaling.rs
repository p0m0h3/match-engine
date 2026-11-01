use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use match_engine::orderbook::OrderBook;
use match_engine::types::{Order, OrderId, Price, Quantity, Side};

fn bench_cancellation_depth(c: &mut Criterion) {
    let mut group = c.benchmark_group("cancel_at_queue_depth");
    for depth in [100, 500, 2000, 5000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(depth), depth, |b, &depth| {
            b.iter_batched(
                || {
                    let mut book = OrderBook::new();
                    // Place all orders at the SAME price level (pure depth)
                    for i in 1..=depth {
                        let order = Order::new_limit(
                            OrderId(i as u64),
                            Side::Buy,
                            Price(1000),
                            Quantity(10),
                        );
                        book.place_order(order);
                    }
                    book
                },
                |mut book| {
                    // Cancel order in the middle of the deep queue
                    black_box(book.cancel_order(black_box(OrderId((depth / 2) as u64))));
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_cancellation_depth);
criterion_main!(benches);
