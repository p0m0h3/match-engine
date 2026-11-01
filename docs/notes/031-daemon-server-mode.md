# 031. Continuous Daemon Server Mode & Graceful Signal Handling

## Decisions Made
1. **Continuous Daemon Server Mode (`src/main.rs`)**:
   - Instead of running a hardcoded script and exiting, the `match-engine` binary now starts as a long-running server daemon.
   - Binds:
     - **Inbound Gateway (Port 9001)**: Listens continuously for external OMS / client connections.
     - **Outbound Broadcaster (Port 9002)**: Streams real-time execution events to connected Kafka adapters and market data subscribers.
2. **Graceful Shutdown via OS Signals (Ctrl+C / SIGINT / SIGTERM)**:
   - Uses `ctrlc` signal handler to trap interrupt signals.
   - Sends a `Shutdown` command through the SPSC ring buffer to the matching core.
   - Flushes WAL and snapshots before clean exit, preventing corrupted state.
3. **Dedicated Demo Binary**:
   - The scripted self-test simulation was moved to `src/bin/demo_simulation.rs` so developers can still run quick mock tests with a single command.
