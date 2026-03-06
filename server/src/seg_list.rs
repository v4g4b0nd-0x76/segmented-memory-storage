use std::{
    alloc::{Layout, alloc_zeroed},
    collections::HashMap,
    ptr,
};

pub struct SegList {
    written: HashMap<String, Vec<*mut u8>>, // each key may have many segments
    // when reading those segments we fre them and return them back to free segs
    free_segs: Vec<*mut u8>,
}
unsafe impl Send for SegList {}
unsafe impl Sync for SegList {}

const SEGMENT_SIZE: usize = 64 * 1024; // 64 kb
const SEGMENT_ALIGN: usize = 4096; // 4 kb
const INITIAL_SPACE: usize = 1024 * 1024 * 1024; // 1 GB
const HEADER_SIZE: usize = 8 + 8 + 4; // 20 bytes fixed header

impl SegList {
    pub async fn new() -> Self {
        let seg_layout = Layout::from_size_align(SEGMENT_SIZE, SEGMENT_ALIGN).unwrap();
        let count = INITIAL_SPACE / SEGMENT_SIZE;
        let mut segs: Vec<*mut u8> = Vec::with_capacity(count);
        for _ in 0..count {
            let ptr = unsafe { alloc_zeroed(seg_layout) };
            if ptr.is_null() {
                panic!("Failed to allocate memory for segment");
            }
            segs.push(ptr);
        }
        SegList {
            written: HashMap::new(),
            free_segs: segs,
        }
    }

    pub async fn push(&mut self, key: String, payload: Vec<u8>) -> Result<(), ListError> {
        if self.free_segs.is_empty() {
            self.grow_segs().await.map_err(|_| ListError::AllocFailed)?;
        }
        let total_entry_size = HEADER_SIZE + payload.len();
        if total_entry_size > SEGMENT_SIZE {
            return Err(ListError::EntryTooLarge);
        }
        let first_free: *mut u8 = self.free_segs.remove(0);
        unsafe {
            let len_bytes = (payload.len() as u32).to_le_bytes();
            ptr::copy_nonoverlapping(len_bytes.as_ptr(), first_free.add(16), 4);
            if !payload.is_empty() {
                ptr::copy_nonoverlapping(
                    payload.as_ptr(),
                    first_free.add(HEADER_SIZE),
                    payload.len(),
                );
            }
        }

        self.written
            .entry(key)
            .or_insert_with(Vec::new)
            .push(first_free);

        Ok(())
    }

    pub async fn pop(&mut self, key: String) -> Result<Vec<u8>, ListError> {
        let segs = self.written.get_mut(&key).ok_or(ListError::KeyNotFound)?;
        if segs.is_empty() {
            return Err(ListError::KeyNotFound);
        }
        let seg_ptr = segs.remove(0);

        let data = unsafe {
            let len = u32::from_le_bytes(*(seg_ptr.add(16) as *const [u8; 4])) as usize;
            let mut buf = vec![0u8; len];
            ptr::copy_nonoverlapping(seg_ptr.add(HEADER_SIZE), buf.as_mut_ptr(), len);
            ptr::write_bytes(seg_ptr, 0, SEGMENT_SIZE);
            buf
        };

        self.free_segs.push(seg_ptr);
        Ok(data)
    }

    pub async fn push_range(&mut self, key: String, items: Vec<Vec<u8>>) -> Result<(), ListError> {
        if self.free_segs.len() < items.len() {
            self.grow_segs().await.map_err(|_| ListError::AllocFailed)?;
        }
        for item in items {
            self.push(key.clone(), item).await?;
        }
        Ok(())
    }
    pub async fn pop_count(
        &mut self,
        key: String,
        count: usize,
    ) -> Result<Vec<Vec<u8>>, ListError> {
        let mut items: Vec<Vec<u8>> = Vec::with_capacity(count);
        for _ in 0..=count {
            items.push(self.pop(key.clone()).await?);
        }
        Ok(items)
    } // pop from start the specified number
    pub async fn pop_range(
        &mut self,
        key: String,
        start: usize,
        end: usize,
    ) -> Result<Vec<Vec<u8>>, ListError> {
        let segs = self.written.get_mut(&key).ok_or(ListError::KeyNotFound)?;

        if end > segs.len() {
            return Err(ListError::OutOfRange);
        }

        let targ: Vec<*mut u8> = segs.drain(start..end).collect();
        let mut result = Vec::with_capacity(targ.len());

        for ptr in &targ {
            let data = unsafe {
                let len = u32::from_le_bytes(*(ptr.add(16) as *const [u8; 4])) as usize;
                let mut buf = vec![0u8; len];
                ptr::copy_nonoverlapping(ptr.add(HEADER_SIZE), buf.as_mut_ptr(), len);
                ptr::write_bytes(*ptr, 0, SEGMENT_SIZE);
                buf
            };
            result.push(data);
            self.free_segs.push(*ptr);
        }

        Ok(result)
    }
    // pop in the range of index
    pub async fn len(&mut self, key: String) -> Result<usize, ListError> {
        let written = self.written.entry(key).or_insert_with(Vec::new);
        Ok(written.len())
    } // len for that key
    pub async fn flush(&mut self, key: String) -> Result<(), ListError> {
        let segs = self.written.get_mut(&key).ok_or(ListError::KeyNotFound)?;

        for ptr in segs.drain(..) {
            unsafe { ptr::write_bytes(ptr, 0, SEGMENT_SIZE) };
            self.free_segs.push(ptr);
        }

        *segs = Vec::new();
        Ok(())
    } // free all written slots for the key
    async fn grow_segs(&mut self) -> Result<(), ListError> {
        let seg_layout = Layout::from_size_align(SEGMENT_SIZE, SEGMENT_ALIGN).unwrap();
        let count = INITIAL_SPACE / SEGMENT_SIZE;
        let mut segs: Vec<*mut u8> = Vec::with_capacity(count);
        for _ in 0..count {
            let ptr = unsafe { alloc_zeroed(seg_layout) };
            if ptr.is_null() {
                panic!("Failed to allocate memory for segment");
            }
            segs.push(ptr);
        }
        self.free_segs.append(&mut segs);
        Ok(())
    }
}

#[derive(Debug)]

pub enum ListError {
    EntryTooLarge,
    AllocFailed,
    KeyNotFound,
    OutOfRange,
}
impl std::fmt::Display for ListError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ListError::EntryTooLarge => write!(f, "entry exceeds segment capacity"),
            ListError::AllocFailed => write!(f, "failed to grow the list"),
            ListError::KeyNotFound => write!(f, "key not found"),
            ListError::OutOfRange => write!(f, "given range is invalidß"),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_full_cycle() {
        let mut list = SegList::new().await;
        let key = "mykey".to_string();

        // push 5 items
        let items: Vec<Vec<u8>> = (0..5u8).map(|i| vec![i; 10]).collect();
        list.push_range(key.clone(), items.clone()).await.unwrap();

        assert_eq!(list.len(key.clone()).await.unwrap(), 5);

        // pop first item
        let first = list.pop(key.clone()).await.unwrap();
        assert_eq!(first, vec![0u8; 10]);
        assert_eq!(list.len(key.clone()).await.unwrap(), 4);

        // pop range index 1..3 (relative to remaining)
        let range = list.pop_range(key.clone(), 0, 2).await.unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0], vec![1u8; 10]);
        assert_eq!(range[1], vec![2u8; 10]);
        assert_eq!(list.len(key.clone()).await.unwrap(), 2);

        // flush remaining
        list.flush(key.clone()).await.unwrap();
        assert_eq!(list.len(key.clone()).await.unwrap(), 0);

        // free_segs should have grown back (5 returned total)
        // push again to verify segments are reusable
        list.push(key.clone(), b"hello".to_vec()).await.unwrap();
        let val = list.pop(key.clone()).await.unwrap();
        assert_eq!(val, b"hello");
    }
}

#[cfg(test)]
mod bench {
    use super::*;
    use std::time::Instant;

    fn pseudo_rand(seed: &mut u64) -> u64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    }

    fn rand_range(seed: &mut u64, min: usize, max: usize) -> usize {
        min + (pseudo_rand(seed) as usize % (max - min))
    }

    fn rand_payload(seed: &mut u64, size: usize) -> Vec<u8> {
        (0..size).map(|_| pseudo_rand(seed) as u8).collect()
    }

    #[tokio::test]
    async fn bench_seglist_random_activity() {
        let mut list = SegList::new().await;
        let mut seed: u64 = 0xdeadbeefcafe1234;

        const KEYS: usize = 200;
        const OPS: usize = 1000_000;

        let keys: Vec<String> = (0..KEYS).map(|i| format!("key:{}", i)).collect();
        let initial_free = list.free_segs.len();
        println!("[BENCH] Initial free segments: {}", initial_free);

        let mut pushes = 0usize;
        let mut pops = 0usize;
        let mut flushes = 0usize;
        let mut errors = 0usize;
        let mut grows = 0usize;

        let start = Instant::now();

        for op in 0..OPS {
            let key = keys[rand_range(&mut seed, 0, KEYS)].clone();
            let action = rand_range(&mut seed, 0, 10);

            match action {
                0..=4 => {
                    let payload_size = rand_range(&mut seed, 1, 1025);
                    let payload = rand_payload(&mut seed, payload_size);
                    let before = list.free_segs.len();
                    match list.push(key, payload).await {
                        Ok(_) => {
                            pushes += 1;
                            if list.free_segs.len() > before {
                                grows += 1;
                            }
                        }
                        Err(_) => errors += 1,
                    }
                }
                5..=6 => {
                    let count = rand_range(&mut seed, 1, 6);
                    let items: Vec<Vec<u8>> = (0..count)
                        .map(|_| {
                            let sz = rand_range(&mut seed, 1, 513);
                            rand_payload(&mut seed, sz)
                        })
                        .collect();
                    match list.push_range(key, items).await {
                        Ok(_) => pushes += count,
                        Err(_) => errors += 1,
                    }
                }
                7 => match list.pop(key).await {
                    Ok(_) => pops += 1,
                    Err(ListError::KeyNotFound) => {}
                    Err(_) => errors += 1,
                },
                8 => {
                    let len = list.len(key.clone()).await.unwrap_or(0);
                    if len >= 2 {
                        let start_idx = rand_range(&mut seed, 0, len - 1);
                        let end_idx = rand_range(&mut seed, start_idx + 1, len + 1);
                        match list.pop_range(key, start_idx, end_idx).await {
                            Ok(items) => pops += items.len(),
                            Err(ListError::KeyNotFound) | Err(ListError::OutOfRange) => {}
                            Err(_) => errors += 1,
                        }
                    }
                }
                _ => match list.flush(key).await {
                    Ok(_) => flushes += 1,
                    Err(ListError::KeyNotFound) => {}
                    Err(_) => errors += 1,
                },
            }

            if op % 10_000 == 0 && op > 0 {
                let total_written: usize = list.written.values().map(|v| v.len()).sum();
                println!(
                    "[BENCH] op={} free_segs={} written_segs={} grows={}",
                    op,
                    list.free_segs.len(),
                    total_written,
                    grows
                );
            }
        }

        let elapsed = start.elapsed();
        let total_written: usize = list.written.values().map(|v| v.len()).sum();
        let final_free = list.free_segs.len();
        let total_segs = total_written + final_free;

        println!(
            "\n[BENCH] {} ops in {:?} ({:.0} ops/sec)",
            OPS,
            elapsed,
            OPS as f64 / elapsed.as_secs_f64()
        );
        println!(
            "[BENCH] pushes={} pops={} flushes={} errors={}",
            pushes, pops, flushes, errors
        );
        println!("[BENCH] grows={}", grows);
        println!(
            "[BENCH] free={} written={} total={} initial={}",
            final_free, total_written, total_segs, initial_free
        );

        assert_eq!(errors, 0, "unexpected errors during random ops");
        assert!(
            total_segs >= initial_free,
            "segment leak detected: {} < {}",
            total_segs,
            initial_free
        );

        println!("\n[BENCH] Verifying segment reuse after full flush...");
        for key in &keys {
            let _ = list.flush(key.clone()).await;
        }
        println!(
            "[BENCH] free_segs after full flush: {}",
            list.free_segs.len()
        );

        list.push("verify".to_string(), b"reuse_check".to_vec())
            .await
            .unwrap();
        let val = list.pop("verify".to_string()).await.unwrap();
        assert_eq!(val, b"reuse_check", "segment reuse corrupted data");
        println!("[BENCH] Segment reuse: OK");
    }
}
