//! Micro-benchmark comparing BTreeMap price-level lookup vs Direct-Indexed FlatTickBook.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use match_engine::arena::OrderArena;
use match_engine::flat_book::FlatTickBook;
use match_engine::types::{Order, OrderId, Price, Quantity, Side};
use std::collections::BTreeMap;

fn bench_btreemap_lookup(c: &mut Criterion) {
    let mut tree = BTreeMap::new();
    for p in 1000..=2000 {
        tree.insert(p, p * 10);
    }

    c.bench_function("btreemap_price_level_lookup", |b| {
        let mut p = 1000;
        b.iter(|| {
            p = 1000 + (p + 1) % 1000;
            black_box(tree.get(&black_box(p)));
        });
    });
}

fn bench_flat_tick_book_lookup(c: &mut Criterion) {
    let mut arena = OrderArena::with_capacity(10_000);
    let mut flat = FlatTickBook::<2000>::new(1000);

    for p in 1000..=2000 {
        let o = Order::new_limit(OrderId(p), Side::Buy, Price(p), Quantity(10));
        let slot = arena.alloc(o).unwrap();
        flat.push_back(&mut arena, Price(p), slot).unwrap();
    }

    c.bench_function("flat_tick_direct_index_lookup", |b| {
        let mut p = 1000;
        b.iter(|| {
            p = 1000 + (p + 1) % 1000;
            black_box(flat.get_level(black_box(Price(p))));
        });
    });
}

fn bench_bbo_search(c: &mut Criterion) {
    let mut arena = OrderArena::with_capacity(10_000);
    let mut flat = FlatTickBook::<2000>::new(1000);

    // Seed orders at various spread intervals
    for p in [1050, 1100, 1150, 1200] {
        let o = Order::new_limit(OrderId(p), Side::Buy, Price(p), Quantity(10));
        let slot = arena.alloc(o).unwrap();
        flat.push_back(&mut arena, Price(p), slot).unwrap();
    }

    c.bench_function("flat_tick_bitmap_best_bid_scan", |b| {
        b.iter(|| {
            black_box(flat.highest_occupied_price());
        });
    });
}

criterion_group!(
    benches,
    bench_btreemap_lookup,
    bench_flat_tick_book_lookup,
    bench_bbo_search
);
criterion_main!(benches);
