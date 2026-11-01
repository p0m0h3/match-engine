//! Cache-line padded, zero-allocation Lock-Free Single-Producer Single-Consumer (SPSC) Ring Buffer.

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Cache-line aligned atomic index preventing False Sharing across CPU cores.
#[repr(align(64))]
struct CacheAlignedAtomicUsize {
    val: AtomicUsize,
}

impl CacheAlignedAtomicUsize {
    const fn new(val: usize) -> Self {
        Self {
            val: AtomicUsize::new(val),
        }
    }
}

/// Internal shared ring buffer storage.
struct RingBufferInner<T, const N: usize> {
    buffer: Box<[UnsafeCell<Option<T>>; N]>,
    tail: CacheAlignedAtomicUsize, // Written by Producer, read by Consumer
    head: CacheAlignedAtomicUsize, // Written by Consumer, read by Producer
}

unsafe impl<T: Send, const N: usize> Send for RingBufferInner<T, N> {}
unsafe impl<T: Send, const N: usize> Sync for RingBufferInner<T, N> {}

/// Producer half of the lock-free SPSC channel.
pub struct Producer<T, const N: usize> {
    inner: Arc<RingBufferInner<T, N>>,
    cached_head: usize,
    tail: usize,
}

unsafe impl<T: Send, const N: usize> Send for Producer<T, N> {}

/// Consumer half of the lock-free SPSC channel.
pub struct Consumer<T, const N: usize> {
    inner: Arc<RingBufferInner<T, N>>,
    cached_tail: usize,
    head: usize,
}

unsafe impl<T: Send, const N: usize> Send for Consumer<T, N> {}

impl<T, const N: usize> Producer<T, N> {
    const MASK: usize = N - 1;

    /// Attempt to push an item without blocking or locking.
    /// Returns Err(item) if the ring buffer is full.
    #[inline(always)]
    pub fn try_push(&mut self, item: T) -> Result<(), T> {
        let next_tail = self.tail + 1;

        if next_tail - self.cached_head > N {
            // Re-read consumer head with Acquire ordering
            self.cached_head = self.inner.head.val.load(Ordering::Acquire);
            if next_tail - self.cached_head > N {
                return Err(item); // Full
            }
        }

        let slot = self.tail & Self::MASK;
        unsafe {
            let ptr = self.inner.buffer[slot].get();
            *ptr = Some(item);
        }

        self.tail = next_tail;
        self.inner.tail.val.store(self.tail, Ordering::Release);
        Ok(())
    }
}

impl<T, const N: usize> Consumer<T, N> {
    const MASK: usize = N - 1;

    /// Attempt to pop an item without blocking or locking.
    /// Returns None if the ring buffer is empty.
    #[inline(always)]
    pub fn try_pop(&mut self) -> Option<T> {
        if self.head == self.cached_tail {
            // Re-read producer tail with Acquire ordering
            self.cached_tail = self.inner.tail.val.load(Ordering::Acquire);
            if self.head == self.cached_tail {
                return None; // Empty
            }
        }

        let slot = self.head & Self::MASK;
        let item = unsafe {
            let ptr = self.inner.buffer[slot].get();
            (*ptr).take()
        };

        self.head += 1;
        self.inner.head.val.store(self.head, Ordering::Release);
        item
    }
}

/// Create a new bounded lock-free SPSC channel.
/// `N` must be a power of two.
pub fn spsc_channel<T, const N: usize>() -> (Producer<T, N>, Consumer<T, N>) {
    assert!(
        N.is_power_of_two(),
        "Ring buffer capacity must be a power of two"
    );

    // Pre-allocate fixed buffer on the heap directly to prevent stack overflow for large capacities
    let mut vec = Vec::with_capacity(N);
    for _ in 0..N {
        vec.push(UnsafeCell::new(None));
    }
    let buffer: Box<[UnsafeCell<Option<T>>; N]> = match vec.into_boxed_slice().try_into() {
        Ok(b) => b,
        Err(_) => unreachable!(),
    };

    let inner = Arc::new(RingBufferInner {
        buffer,
        tail: CacheAlignedAtomicUsize::new(0),
        head: CacheAlignedAtomicUsize::new(0),
    });

    (
        Producer {
            inner: Arc::clone(&inner),
            cached_head: 0,
            tail: 0,
        },
        Consumer {
            inner,
            cached_tail: 0,
            head: 0,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_spsc_single_thread() {
        let (mut producer, mut consumer) = spsc_channel::<u64, 4>();

        assert_eq!(consumer.try_pop(), None);
        assert!(producer.try_push(10).is_ok());
        assert!(producer.try_push(20).is_ok());
        assert_eq!(consumer.try_pop(), Some(10));
        assert_eq!(consumer.try_pop(), Some(20));
        assert_eq!(consumer.try_pop(), None);
    }

    #[test]
    fn test_spsc_cross_thread_burst() {
        const CAPACITY: usize = 1024;
        const NUM_ITEMS: usize = 100_000;

        let (mut producer, mut consumer) = spsc_channel::<usize, CAPACITY>();

        let producer_handle = thread::spawn(move || {
            for i in 0..NUM_ITEMS {
                let mut item = i;
                loop {
                    match producer.try_push(item) {
                        Ok(()) => break,
                        Err(returned) => {
                            item = returned;
                            core::hint::spin_loop();
                        }
                    }
                }
            }
        });

        let consumer_handle = thread::spawn(move || {
            let mut received = Vec::with_capacity(NUM_ITEMS);
            while received.len() < NUM_ITEMS {
                if let Some(item) = consumer.try_pop() {
                    received.push(item);
                } else {
                    core::hint::spin_loop();
                }
            }
            received
        });

        producer_handle.join().unwrap();
        let received = consumer_handle.join().unwrap();

        for (i, val) in received.into_iter().enumerate() {
            assert_eq!(i, val);
        }
    }
}
