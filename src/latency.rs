//! Hardware CPU cycle timer and high-resolution latency histogram for Tick-to-Trade profiling.

/// Read the current hardware CPU counter with single-instruction latency (< 2 ns).
#[inline(always)]
pub fn read_hardware_counter() -> u64 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_rdtsc()
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        let count: u64;
        core::arch::asm!("mrs {}, cntvct_el0", out(reg) count, options(nomem, nostack));
        count
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}

/// Query hardware timer frequency in Hz (ticks per second).
#[inline(always)]
pub fn hardware_timer_frequency_hz() -> u64 {
    #[cfg(target_arch = "aarch64")]
    unsafe {
        let freq: u64;
        core::arch::asm!("mrs {}, cntfrq_el0", out(reg) freq, options(nomem, nostack));
        freq
    }

    #[cfg(not(target_arch = "aarch64"))]
    {
        // Default estimate: 3.0 GHz for modern x86 cores
        3_000_000_000
    }
}

/// Convert elapsed hardware ticks into nanoseconds.
#[inline(always)]
pub fn cycles_to_nanos(cycles: u64, freq_hz: u64) -> u64 {
    if freq_hz == 0 {
        return 0;
    }
    ((cycles as u128 * 1_000_000_000) / freq_hz as u128) as u64
}

/// Zero-allocation, high-resolution latency histogram tracking distributions from 1 ns to 100 µs.
pub struct LatencyHistogram {
    // 100,000 bins covering 0 ns to 100,000 ns (100 µs) in 1 ns increments
    // Latencies > 100 µs fall into the overflow bin.
    bins: Vec<u32>,
    overflow_bin: u32,
    total_samples: u64,
    min_ns: u64,
    max_ns: u64,
    sum_ns: u128,
}

impl LatencyHistogram {
    pub const MAX_TRACKED_NS: usize = 100_000;

    pub fn new() -> Self {
        Self {
            bins: vec![0; Self::MAX_TRACKED_NS],
            overflow_bin: 0,
            total_samples: 0,
            min_ns: u64::MAX,
            max_ns: 0,
            sum_ns: 0,
        }
    }

    #[inline]
    pub fn record(&mut self, nanos: u64) {
        self.total_samples += 1;
        self.sum_ns += nanos as u128;
        if nanos < self.min_ns {
            self.min_ns = nanos;
        }
        if nanos > self.max_ns {
            self.max_ns = nanos;
        }

        if (nanos as usize) < Self::MAX_TRACKED_NS {
            self.bins[nanos as usize] += 1;
        } else {
            self.overflow_bin += 1;
        }
    }

    pub fn mean_ns(&self) -> f64 {
        if self.total_samples == 0 {
            0.0
        } else {
            self.sum_ns as f64 / self.total_samples as f64
        }
    }

    pub fn percentile(&self, p: f64) -> u64 {
        if self.total_samples == 0 {
            return 0;
        }

        let target_rank = ((p / 100.0) * self.total_samples as f64).ceil() as u64;
        let mut accumulated = 0u64;

        for (ns, count) in self.bins.iter().enumerate() {
            accumulated += *count as u64;
            if accumulated >= target_rank {
                return ns as u64;
            }
        }

        self.max_ns
    }

    pub fn min_ns(&self) -> u64 {
        if self.total_samples == 0 { 0 } else { self.min_ns }
    }

    pub fn max_ns(&self) -> u64 {
        self.max_ns
    }

    pub fn total_samples(&self) -> u64 {
        self.total_samples
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hardware_timer_monotonic() {
        let t0 = read_hardware_counter();
        let mut sum = 0u64;
        for i in 0..1000 {
            sum ^= i;
        }
        std::hint::black_box(sum);
        let t1 = read_hardware_counter();

        assert!(t1 >= t0);
        let freq = hardware_timer_frequency_hz();
        assert!(freq > 0);
        let nanos = cycles_to_nanos(t1 - t0, freq);
        assert!(nanos < 1_000_000); // Definitely less than 1ms
    }

    #[test]
    fn test_histogram_percentiles() {
        let mut hist = LatencyHistogram::new();
        // Record 1..=100 ns
        for i in 1..=100 {
            hist.record(i);
        }

        assert_eq!(hist.min_ns(), 1);
        assert_eq!(hist.max_ns(), 100);
        assert_eq!(hist.percentile(50.0), 50);
        assert_eq!(hist.percentile(90.0), 90);
        assert_eq!(hist.percentile(99.0), 99);
    }
}
