//! Binary-aligned Write-Ahead Log (WAL) and Deterministic Replay Engine.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use crate::engine::EngineCommand;
use crate::orderbook::OrderBook;
use crate::types::{Order, OrderId, OrderType, Price, Quantity, Side, TimeInForce};

/// Magic header bytes identifying a valid match-engine WAL file ("MEWAL001").
pub const WAL_MAGIC: [u8; 8] = *b"MEWAL001";

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalCommandType {
    PlaceOrder = 1,
    CancelOrder = 2,
}

/// Fixed-size binary representation of an engine command for zero-allocation logging.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalRecord {
    pub sequence_id: u64,
    pub timestamp_ns: u64,
    pub command_type: u8,
    pub order_id: u64,
    pub side: u8,
    pub order_type: u8,
    pub time_in_force: u8,
    pub price: u64,
    pub quantity: u64,
    pub checksum: u32,
}

impl WalRecord {
    pub fn from_command(seq: u64, ts_ns: u64, cmd: &EngineCommand) -> Option<Self> {
        let (cmd_type, order_id, side, o_type, tif, price, qty) = match cmd {
            EngineCommand::PlaceOrder(o) => (
                WalCommandType::PlaceOrder as u8,
                o.id.0,
                o.side as u8,
                o.order_type as u8,
                o.time_in_force as u8,
                o.price.0,
                o.initial_quantity.0,
            ),
            EngineCommand::CancelOrder(id) => (
                WalCommandType::CancelOrder as u8,
                id.0,
                0,
                0,
                0,
                0,
                0,
            ),
            EngineCommand::Shutdown => return None,
        };

        // Compute fast checksum across fields
        let mut checksum = (seq ^ ts_ns ^ order_id ^ price ^ qty) as u32;
        checksum ^= ((cmd_type as u32) << 24)
            | ((side as u32) << 16)
            | ((o_type as u32) << 8)
            | (tif as u32);

        Some(Self {
            sequence_id: seq,
            timestamp_ns: ts_ns,
            command_type: cmd_type,
            order_id,
            side,
            order_type: o_type,
            time_in_force: tif,
            price,
            quantity: qty,
            checksum,
        })
    }

    pub fn to_command(&self) -> Result<EngineCommand, &'static str> {
        // Validate checksum
        let mut expected = (self.sequence_id ^ self.timestamp_ns ^ self.order_id ^ self.price ^ self.quantity) as u32;
        expected ^= ((self.command_type as u32) << 24)
            | ((self.side as u32) << 16)
            | ((self.order_type as u32) << 8)
            | (self.time_in_force as u32);

        if self.checksum != expected {
            return Err("WAL record checksum mismatch (data corruption detected)");
        }

        if self.command_type == WalCommandType::PlaceOrder as u8 {
            let side = match self.side {
                0 => Side::Buy,
                1 => Side::Sell,
                _ => return Err("Invalid side"),
            };
            let order_type = match self.order_type {
                0 => OrderType::Limit,
                1 => OrderType::Market,
                _ => return Err("Invalid order type"),
            };
            let time_in_force = match self.time_in_force {
                0 => TimeInForce::Gtc,
                1 => TimeInForce::Ioc,
                2 => TimeInForce::Fok,
                3 => TimeInForce::PostOnly,
                _ => return Err("Invalid time in force"),
            };

            Ok(EngineCommand::PlaceOrder(Order::new(
                OrderId(self.order_id),
                side,
                order_type,
                time_in_force,
                Price(self.price),
                Quantity(self.quantity),
            )))
        } else if self.command_type == WalCommandType::CancelOrder as u8 {
            Ok(EngineCommand::CancelOrder(OrderId(self.order_id)))
        } else {
            Err("Unknown command type in WAL")
        }
    }
}

/// Binary Write-Ahead Log writer.
pub struct WalWriter {
    writer: BufWriter<File>,
    current_seq: u64,
}

impl WalWriter {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file_exists = path.as_ref().exists();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        let mut writer = BufWriter::with_capacity(64 * 1024, file);

        if !file_exists {
            writer.write_all(&WAL_MAGIC)?;
            writer.flush()?;
        }

        Ok(Self {
            writer,
            current_seq: 0,
        })
    }

    #[inline]
    pub fn append(&mut self, timestamp_ns: u64, cmd: &EngineCommand) -> io::Result<Option<u64>> {
        self.current_seq += 1;
        let record = match WalRecord::from_command(self.current_seq, timestamp_ns, cmd) {
            Some(r) => r,
            None => return Ok(None),
        };

        // Write raw struct bytes directly (zero-copy serialization)
        let slice = unsafe {
            std::slice::from_raw_parts(
                (&record as *const WalRecord) as *const u8,
                std::mem::size_of::<WalRecord>(),
            )
        };

        self.writer.write_all(slice)?;
        Ok(Some(self.current_seq))
    }

    #[inline]
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Replays a WAL file into an order book to recover the exact pre-crash state.
pub struct WalReader;

impl WalReader {
    pub fn replay<P: AsRef<Path>>(path: P, book: &mut OrderBook) -> io::Result<u64> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if magic != WAL_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid WAL magic header",
            ));
        }

        let record_size = std::mem::size_of::<WalRecord>();
        let mut buffer = vec![0u8; record_size];
        let mut last_seq = 0;

        loop {
            match reader.read_exact(&mut buffer) {
                Ok(()) => {
                    let record: WalRecord = unsafe { std::ptr::read(buffer.as_ptr() as *const WalRecord) };
                    match record.to_command() {
                        Ok(cmd) => match cmd {
                            EngineCommand::PlaceOrder(order) => {
                                book.place_order(order);
                                last_seq = record.sequence_id;
                            }
                            EngineCommand::CancelOrder(id) => {
                                book.cancel_order(id);
                                last_seq = record.sequence_id;
                            }
                            EngineCommand::Shutdown => {}
                        },
                        Err(e) => {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    break; // End of WAL reached cleanly
                }
                Err(e) => return Err(e),
            }
        }

        Ok(last_seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_write_and_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        // 1. Write commands to WAL
        {
            let mut wal = WalWriter::open(&wal_path).unwrap();

            let cmd1 = EngineCommand::PlaceOrder(Order::new_limit(
                OrderId(1),
                Side::Sell,
                Price(100),
                Quantity(50),
            ));
            let cmd2 = EngineCommand::PlaceOrder(Order::new_limit(
                OrderId(2),
                Side::Buy,
                Price(90),
                Quantity(30),
            ));
            let cmd3 = EngineCommand::PlaceOrder(Order::new_limit(
                OrderId(3),
                Side::Buy,
                Price(100),
                Quantity(20),
            ));

            wal.append(1000, &cmd1).unwrap();
            wal.append(2000, &cmd2).unwrap();
            wal.append(3000, &cmd3).unwrap();
            wal.flush().unwrap();
        }

        // 2. Recover into a brand new empty OrderBook
        let mut recovered_book = OrderBook::new();
        let last_seq = WalReader::replay(&wal_path, &mut recovered_book).unwrap();

        assert_eq!(last_seq, 3);
        // Order 1 was 50 @ 100, Order 3 took 20 @ 100 -> remaining ask is 30 @ 100
        assert_eq!(recovered_book.best_ask(), Some(Price(100)));
        assert_eq!(recovered_book.best_bid(), Some(Price(90)));

        // Verify remaining volume in the price level
        let l2 = recovered_book.get_l2_snapshot::<1>();
        assert_eq!(l2.asks[0].price, Price(100));
        assert_eq!(l2.asks[0].total_quantity, Quantity(30));
    }
}
