//! Fast binary state snapshot checkpointing for fast crash recovery and WAL truncation.

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::orderbook::OrderBook;
use crate::types::Order;

/// Magic header for snapshot files ("MESNAP01").
pub const SNAPSHOT_MAGIC: [u8; 8] = *b"MESNAP01";

/// Header metadata written at the beginning of each snapshot file.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotHeader {
    pub magic: [u8; 8],
    pub last_sequence_id: u64,
    pub order_count: u64,
    pub checksum: u64,
}

pub struct SnapshotManager {
    dir: PathBuf,
}

impl SnapshotManager {
    pub fn new<P: AsRef<Path>>(dir: P) -> Self {
        fs::create_dir_all(dir.as_ref()).ok();
        Self {
            dir: dir.as_ref().to_path_buf(),
        }
    }

    /// Save an atomic binary snapshot of all active resting orders in the order book.
    pub fn save_snapshot(&self, book: &OrderBook, last_seq: u64) -> io::Result<PathBuf> {
        let order_size = std::mem::size_of::<Order>();
        let mut active_orders = Vec::with_capacity(book.order_locations.len());

        // Extract resting orders
        for (_, (_, _, slot)) in &book.order_locations {
            let order = book.arena.get(*slot).order;
            active_orders.push(order);
        }

        let order_count = active_orders.len() as u64;
        let mut checksum = last_seq ^ order_count;
        for o in &active_orders {
            checksum ^= o.id.0 ^ o.price.0 ^ o.leaves_quantity.0;
        }

        let header = SnapshotHeader {
            magic: SNAPSHOT_MAGIC,
            last_sequence_id: last_seq,
            order_count,
            checksum,
        };

        let final_path = self.dir.join(format!("snapshot_{:012}.bin", last_seq));
        let tmp_path = self.dir.join(format!("snapshot_{:012}.tmp", last_seq));

        {
            let file = File::create(&tmp_path)?;
            let mut writer = BufWriter::with_capacity(64 * 1024, file);

            // 1. Write Header
            let header_bytes = unsafe {
                std::slice::from_raw_parts(
                    (&header as *const SnapshotHeader) as *const u8,
                    std::mem::size_of::<SnapshotHeader>(),
                )
            };
            writer.write_all(header_bytes)?;

            // 2. Write Orders
            for order in &active_orders {
                let order_bytes = unsafe {
                    std::slice::from_raw_parts(
                        (order as *const Order) as *const u8,
                        order_size,
                    )
                };
                writer.write_all(order_bytes)?;
            }

            writer.flush()?;
        }

        // Atomic rename guarantees no half-written snapshots on crash
        fs::rename(&tmp_path, &final_path)?;

        Ok(final_path)
    }

    /// Find the latest valid snapshot file in the directory.
    pub fn find_latest_snapshot(&self) -> io::Result<Option<PathBuf>> {
        let mut latest: Option<(u64, PathBuf)> = None;

        if !self.dir.exists() {
            return Ok(None);
        }

        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("bin") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Some(seq_str) = stem.strip_prefix("snapshot_") {
                        if let Ok(seq) = seq_str.parse::<u64>() {
                            if latest.as_ref().map_or(true, |(curr_seq, _)| seq > *curr_seq) {
                                latest = Some((seq, path));
                            }
                        }
                    }
                }
            }
        }

        Ok(latest.map(|(_, p)| p))
    }

    /// Load and restore the state of the OrderBook from a snapshot file.
    pub fn load_snapshot<P: AsRef<Path>>(path: P, book: &mut OrderBook) -> io::Result<u64> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut header_buf = vec![0u8; std::mem::size_of::<SnapshotHeader>()];
        reader.read_exact(&mut header_buf)?;

        let header: SnapshotHeader = unsafe {
            std::ptr::read(header_buf.as_ptr() as *const SnapshotHeader)
        };

        if header.magic != SNAPSHOT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid snapshot magic header",
            ));
        }

        let order_size = std::mem::size_of::<Order>();
        let mut order_buf = vec![0u8; order_size];
        let mut computed_checksum = header.last_sequence_id ^ header.order_count;

        for _ in 0..header.order_count {
            reader.read_exact(&mut order_buf)?;
            let order: Order = unsafe {
                std::ptr::read(order_buf.as_ptr() as *const Order)
            };

            computed_checksum ^= order.id.0 ^ order.price.0 ^ order.leaves_quantity.0;

            // Rest the order directly in the book
            let order_id = order.id;
            let price = order.price;
            let side = order.side;

            let slot = book.arena.alloc(order).map_err(|e| {
                io::Error::new(io::ErrorKind::OutOfMemory, e)
            })?;

            book.order_locations.insert(order_id, (price, side, slot));

            match side {
                crate::types::Side::Buy => {
                    book.bids
                        .entry(std::cmp::Reverse(price))
                        .or_default()
                        .push_back(&mut book.arena, slot);
                }
                crate::types::Side::Sell => {
                    book.asks
                        .entry(price)
                        .or_default()
                        .push_back(&mut book.arena, slot);
                }
            }
        }

        if computed_checksum != header.checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Snapshot checksum mismatch (file corrupted)",
            ));
        }

        Ok(header.last_sequence_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OrderId, Price, Quantity, Side};

    #[test]
    fn test_snapshot_save_and_restore() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SnapshotManager::new(dir.path());

        // 1. Setup order book with 3 resting orders
        let mut book = OrderBook::new();
        book.place_order(Order::new_limit(OrderId(1), Side::Buy, Price(100), Quantity(10)));
        book.place_order(Order::new_limit(OrderId(2), Side::Buy, Price(99), Quantity(25)));
        book.place_order(Order::new_limit(OrderId(3), Side::Sell, Price(105), Quantity(50)));

        assert_eq!(book.best_bid(), Some(Price(100)));
        assert_eq!(book.best_ask(), Some(Price(105)));

        // 2. Save snapshot at sequence 42
        let path = mgr.save_snapshot(&book, 42).unwrap();
        assert!(path.exists());

        // 3. Restore snapshot into a clean new OrderBook
        let mut restored_book = OrderBook::new();
        let recovered_seq = SnapshotManager::load_snapshot(&path, &mut restored_book).unwrap();

        assert_eq!(recovered_seq, 42);
        assert_eq!(restored_book.best_bid(), Some(Price(100)));
        assert_eq!(restored_book.best_ask(), Some(Price(105)));
        assert_eq!(restored_book.order_locations.len(), 3);

        // Check depth volume matches
        let l2 = restored_book.get_l2_snapshot::<5>();
        assert_eq!(l2.bids[0].price, Price(100));
        assert_eq!(l2.bids[0].total_quantity, Quantity(10));
        assert_eq!(l2.asks[0].price, Price(105));
        assert_eq!(l2.asks[0].total_quantity, Quantity(50));
    }
}
