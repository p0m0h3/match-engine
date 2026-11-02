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

impl SnapshotHeader {
    pub const SIZE: usize = 32;

    /// Serialize header to a 32-byte little-endian byte array.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.magic);
        buf[8..16].copy_from_slice(&self.last_sequence_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.order_count.to_le_bytes());
        buf[24..32].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    /// Deserialize header from a 32-byte little-endian byte array.
    pub fn from_bytes(buf: &[u8; Self::SIZE]) -> Self {
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);
        Self {
            magic,
            last_sequence_id: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            order_count: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            checksum: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
        }
    }
}

const _: () = assert!(std::mem::size_of::<SnapshotHeader>() == SnapshotHeader::SIZE);

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
        let mut active_orders = Vec::with_capacity(book.order_locations.len());

        // Extract resting orders
        for (_, _, slot) in book.order_locations.values() {
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
            writer.write_all(&header.to_bytes())?;

            // 2. Write Orders
            for order in &active_orders {
                writer.write_all(&order.to_bytes())?;
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
            if path.extension().and_then(|s| s.to_str()) == Some("bin")
                && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                && let Some(seq_str) = stem.strip_prefix("snapshot_")
                && let Ok(seq) = seq_str.parse::<u64>()
                && latest.as_ref().is_none_or(|(curr_seq, _)| seq > *curr_seq)
            {
                latest = Some((seq, path));
            }
        }

        Ok(latest.map(|(_, p)| p))
    }

    /// Load and restore the state of the OrderBook from a snapshot file.
    pub fn load_snapshot<P: AsRef<Path>>(path: P, book: &mut OrderBook) -> io::Result<u64> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut header_buf = [0u8; SnapshotHeader::SIZE];
        reader.read_exact(&mut header_buf)?;

        let header = SnapshotHeader::from_bytes(&header_buf);

        if header.magic != SNAPSHOT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid snapshot magic header",
            ));
        }

        let mut order_buf = [0u8; Order::SERIALIZED_SIZE];
        let mut computed_checksum = header.last_sequence_id ^ header.order_count;

        for _ in 0..header.order_count {
            reader.read_exact(&mut order_buf)?;
            let order = Order::from_bytes(&order_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            computed_checksum ^= order.id.0 ^ order.price.0 ^ order.leaves_quantity.0;

            // Rest the order directly in the book
            let order_id = order.id;
            let price = order.price;
            let side = order.side;

            let slot = book
                .arena
                .alloc(order)
                .map_err(|e| io::Error::new(io::ErrorKind::OutOfMemory, e))?;

            book.order_locations.insert(order_id, (price, side, slot));

            match side {
                crate::types::Side::Buy => {
                    if book.flat_bids.in_bounds(price) {
                        book.flat_bids
                            .push_back(&mut book.arena, price, slot)
                            .unwrap();
                    } else {
                        book.sparse_bids
                            .entry(std::cmp::Reverse(price))
                            .or_default()
                            .push_back(&mut book.arena, slot);
                    }
                }
                crate::types::Side::Sell => {
                    if book.flat_asks.in_bounds(price) {
                        book.flat_asks
                            .push_back(&mut book.arena, price, slot)
                            .unwrap();
                    } else {
                        book.sparse_asks
                            .entry(price)
                            .or_default()
                            .push_back(&mut book.arena, slot);
                    }
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
        book.place_order(Order::new_limit(
            OrderId(1),
            Side::Buy,
            Price(100),
            Quantity(10),
        ));
        book.place_order(Order::new_limit(
            OrderId(2),
            Side::Buy,
            Price(99),
            Quantity(25),
        ));
        book.place_order(Order::new_limit(
            OrderId(3),
            Side::Sell,
            Price(105),
            Quantity(50),
        ));

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

    #[test]
    fn test_snapshot_header_bytes_roundtrip() {
        let header = SnapshotHeader {
            magic: SNAPSHOT_MAGIC,
            last_sequence_id: 12345,
            order_count: 50,
            checksum: 987654321,
        };
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), SnapshotHeader::SIZE);

        let decoded = SnapshotHeader::from_bytes(&bytes);
        assert_eq!(header, decoded);
    }
}
