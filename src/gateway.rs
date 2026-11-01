//! Dual-port TCP Gateway providing independent Inbound (Orders) and Outbound (Events) network interfaces.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::engine::{EngineCommand, EngineEvent};
use crate::ring_buffer::{Consumer, Producer};
use crate::wire::{WireCommandFrame, WireEventFrame};

/// Start the Inbound Order Ingestion TCP server (e.g., listening for OMS connections).
pub fn start_inbound_gateway<const N: usize>(
    addr: &str,
    mut producer: Producer<EngineCommand, N>,
    running: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    let listener = TcpListener::bind(addr).expect("Failed to bind inbound TCP gateway");
    listener.set_nonblocking(true).unwrap();

    thread::spawn(move || {
        let frame_size = WireCommandFrame::SIZE;
        let mut buf = vec![0u8; frame_size];

        while running.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let _ = stream.set_nonblocking(false);
                    let _ = stream.set_nodelay(true);
                    let _ = stream.set_read_timeout(None);
                    let mut reader = std::io::BufReader::with_capacity(65_536, stream);

                    while running.load(Ordering::Relaxed) {
                        match reader.read_exact(&mut buf) {
                            Ok(()) => {
                                let mut arr = [0u8; WireCommandFrame::SIZE];
                                arr.copy_from_slice(&buf);
                                let frame = WireCommandFrame::from_bytes(&arr);

                                if let Ok(cmd) = frame.to_engine_command() {
                                    let is_shutdown = matches!(cmd, EngineCommand::Shutdown);
                                    while let Err(returned) = producer.try_push(cmd.clone()) {
                                        let _ = returned;
                                        core::hint::spin_loop();
                                    }
                                    if is_shutdown {
                                        return;
                                    }
                                }
                            }
                            Err(_) => break, // Client disconnected or EOF
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(_) => break,
            }
        }
    })
}

/// Start the Outbound Event Broadcaster TCP server (e.g. for Kafka Translator / Market Data).
pub fn start_outbound_broadcaster<const N: usize>(
    addr: &str,
    mut consumer: Consumer<EngineEvent, N>,
    running: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    let listener = TcpListener::bind(addr).expect("Failed to bind outbound TCP broadcaster");
    listener.set_nonblocking(true).unwrap();

    thread::spawn(move || {
        let mut connected_clients: Vec<TcpStream> = Vec::new();
        let mut sequence_id: u64 = 0;
        let mut idle_spins: usize = 0;

        while running.load(Ordering::Relaxed) {
            // Accept new translator subscribers non-blocking
            if let Ok((stream, _)) = listener.accept() {
                let _ = stream.set_nonblocking(false);
                let _ = stream.set_nodelay(true);
                connected_clients.push(stream);
            }

            // Consume emitted events from engine ring buffer
            if let Some(event) = consumer.try_pop() {
                idle_spins = 0;
                sequence_id += 1;
                let ts_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;

                let is_shutdown = matches!(event, EngineEvent::EngineShutdown);
                let wire_frame = WireEventFrame::from_engine_event(sequence_id, ts_ns, &event);
                let bytes = wire_frame.as_bytes();

                // Fan-out to all connected subscribers (e.g., Kafka Bridge)
                connected_clients.retain_mut(|client| {
                    client.write_all(bytes).is_ok()
                });

                if is_shutdown {
                    break;
                }
            } else {
                idle_spins += 1;
                if idle_spins < 64 {
                    core::hint::spin_loop();
                } else if idle_spins < 512 {
                    thread::yield_now();
                } else {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::run_matching_engine_loop;
    use crate::ring_buffer::spsc_channel;

    #[test]
    fn test_dual_port_tcp_integration() {
        const CAPACITY: usize = 1024;
        let (inbound_producer, engine_cmd_consumer) = spsc_channel::<EngineCommand, CAPACITY>();
        let (engine_event_producer, outbound_consumer) = spsc_channel::<EngineEvent, CAPACITY>();

        let running = Arc::new(AtomicBool::new(true));

        let in_addr = "127.0.0.1:19001";
        let out_addr = "127.0.0.1:19002";

        // 1. Start Inbound Gateway
        let in_handle = start_inbound_gateway(in_addr, inbound_producer, Arc::clone(&running));

        // 2. Start Outbound Broadcaster
        let out_handle = start_outbound_broadcaster(out_addr, outbound_consumer, Arc::clone(&running));

        // 3. Start Core Engine Loop
        let r_engine = Arc::clone(&running);
        let engine_handle = thread::spawn(move || {
            run_matching_engine_loop(engine_cmd_consumer, engine_event_producer, r_engine);
        });

        thread::sleep(Duration::from_millis(50));

        // 4. Connect simulated Kafka Translator / Subscriber to Outbound Port
        println!("Connecting subscriber...");
        let mut subscriber = TcpStream::connect(out_addr).expect("Translator failed to connect");
        subscriber.set_nodelay(true).unwrap();

        // 5. Connect simulated OMS to Inbound Port
        println!("Connecting OMS...");
        let mut oms_client = TcpStream::connect(in_addr).expect("OMS failed to connect");
        oms_client.set_nodelay(true).unwrap();

        // 6. OMS sends a Limit Order over TCP
        println!("Sending order...");
        let order_frame = WireCommandFrame::new_place_limit(101, 0, 500, 10, 9999);
        oms_client.write_all(order_frame.as_bytes()).unwrap();

        // 7. Read event from Outbound Port (Translator receives it)
        println!("Reading event...");
        let mut event_buf = [0u8; WireEventFrame::SIZE];
        subscriber.read_exact(&mut event_buf).unwrap();
        println!("Event read successfully!");
        let received_event = WireEventFrame::from_bytes(&event_buf);

        assert_eq!(received_event.sequence_id, 1);
        assert_eq!(received_event.event_type, 1); // OrderAccepted
        assert_eq!(received_event.order_id, 101);

        // 8. OMS sends Shutdown
        oms_client.write_all(WireCommandFrame::new_shutdown().as_bytes()).unwrap();

        // 9. Read the shutdown event on subscriber to let outbound thread finish
        subscriber.read_exact(&mut event_buf).unwrap();

        running.store(false, Ordering::Relaxed);
        engine_handle.join().unwrap();
        in_handle.join().unwrap();
        out_handle.join().unwrap();
    }
}
