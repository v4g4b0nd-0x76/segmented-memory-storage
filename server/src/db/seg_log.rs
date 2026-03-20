use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    collections::BTreeMap,
    ptr,
};

const SEGMENT_SIZE: usize = 64 * 1024;
const SEGMENT_ALIGN: usize = 4096;
const INITIAL_SEGMENTS: usize = 8;
const MAX_SEGMENTS: usize = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
struct EntryLoc {
    segment_idx: usize,
    offset: usize,
    len: usize,
}

const HEADER_SIZE: usize = 8 + 8 + 4;

pub struct SegLog {
    segs: Vec<*mut u8>,
    active_seg: usize,
    write_cursor: usize,
    idx: BTreeMap<u64, EntryLoc>,
    next_id: u64,
    entry_count: usize,
    seg_layout: Layout,
    lock_write: tokio::sync::Mutex<()>,
}

unsafe impl Send for SegLog {}
unsafe impl Sync for SegLog {}

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub id: u64,
    pub timestamp: u64,
    pub len: usize,
    pub payload: Vec<u8>,
}

impl SegLog {
    pub fn new() -> Self {
        let seg_layout = Layout::from_size_align(SEGMENT_SIZE, SEGMENT_ALIGN).unwrap();
        let mut segs: Vec<*mut u8> = Vec::with_capacity(MAX_SEGMENTS);
        for _ in 0..INITIAL_SEGMENTS {
            let ptr = unsafe { alloc_zeroed(seg_layout) };

            if ptr.is_null() {
                panic!("failed to allocate segment");
            }
            segs.push(ptr);
        }
        SegLog {
            segs,
            active_seg: 0,
            write_cursor: 0,
            seg_layout,
            idx: BTreeMap::new(),
            lock_write: tokio::sync::Mutex::new(()),
            next_id: 1,
            entry_count: 0,
        }
    }

    pub async fn allocate_seg(&mut self) -> Result<(), LogError> {
        if self.segs.len() >= MAX_SEGMENTS {
            return Err(LogError::MaxSegmentsReached);
        }
        let ptr = unsafe { alloc_zeroed(self.seg_layout) };
        if ptr.is_null() {
            return Err(LogError::AllocationFailed);
        }
        self.segs.push(ptr);
        Ok(())
    }

    pub async fn append(&mut self, timestamp: u64, payload: &[u8]) -> Result<u64, LogError> {
        let total_entry_size = HEADER_SIZE + payload.len();
        if total_entry_size > SEGMENT_SIZE {
            return Err(LogError::EntryTooLarge);
        }
        if self.write_cursor + total_entry_size > SEGMENT_SIZE {
            self.active_seg += 1;
            self.write_cursor = 0;
            if self.active_seg >= self.segs.len() {
                self.allocate_seg().await?;
            }
        }

        let _lock = match self.lock_write.try_lock() {
            Ok(l) => l,
            Err(_) => return Err(LogError::WriteLockUnavailable),
        };

        let entry_id = self.next_id;
        let dst = unsafe { self.segs[self.active_seg].add(self.write_cursor) };
        unsafe {
            ptr::copy_nonoverlapping(entry_id.to_le_bytes().as_ptr(), dst, 8);
            ptr::copy_nonoverlapping(timestamp.to_le_bytes().as_ptr(), dst.add(8), 8);
            ptr::copy_nonoverlapping(
                (payload.len() as u32).to_le_bytes().as_ptr(),
                dst.add(16),
                4,
            );
            if !payload.is_empty() {
                ptr::copy_nonoverlapping(payload.as_ptr(), dst.add(HEADER_SIZE), payload.len());
            }
        }

        self.idx.insert(
            entry_id,
            EntryLoc {
                segment_idx: self.active_seg,
                offset: self.write_cursor,
                len: total_entry_size,
            },
        );
        self.write_cursor += total_entry_size;
        self.next_id += 1;
        self.entry_count += 1;
        Ok(entry_id)
    }

    pub fn read(&self, entry_id: u64) -> Result<(u64, u64, Vec<u8>), LogError> {
        let loc = self.idx.get(&entry_id).ok_or(LogError::EntryNotFound)?;
        let src = unsafe { self.segs[loc.segment_idx].add(loc.offset) as *const u8 };
        unsafe {
            let id = u64::from_le_bytes(*(src as *const [u8; 8]));
            let ts = u64::from_le_bytes(*(src.add(8) as *const [u8; 8]));
            let len = u32::from_le_bytes(*(src.add(16) as *const [u8; 4])) as usize;
            let mut payload = vec![0u8; len];
            if len > 0 {
                ptr::copy_nonoverlapping(src.add(HEADER_SIZE), payload.as_mut_ptr(), len);
            }
            Ok((id, ts, payload))
        }
    }

    pub fn read_range(&self, start: u64, end: u64) -> Vec<(u64, u64, Vec<u8>)> {
        self.idx
            .range(start..=end)
            .filter_map(|(&id, _)| self.read(id).ok())
            .collect()
    }

    pub fn trim(&mut self, cutoff_id: u64) -> Vec<u64> {
        let to_remove: Vec<u64> = self.idx.range(..cutoff_id).map(|(&id, _)| id).collect();
        for id in &to_remove {
            self.idx.remove(id);
            self.entry_count -= 1;
        }
        if let Some((_, first_loc)) = self.idx.iter().next() {
            let first_live = first_loc.segment_idx;
            for seg_idx in 0..first_live {
                unsafe {
                    ptr::write_bytes(self.segs[seg_idx], 0, SEGMENT_SIZE);
                }
            }
        }
        to_remove
    }

    pub fn get_raw_entry_slice(&self, entry_id: u64) -> Result<&[u8], &'static str> {
        let loc = self.idx.get(&entry_id).ok_or("entry not found")?;
        unsafe {
            Ok(std::slice::from_raw_parts(
                self.segs[loc.segment_idx].add(loc.offset),
                loc.len,
            ))
        }
    }

    pub fn total_entries(&self) -> u64 {
        self.entry_count as u64
    }
    pub fn total_segments(&self) -> usize {
        self.segs.len()
    }
    pub fn active_segment_usage(&self) -> f64 {
        (self.write_cursor as f64 / SEGMENT_SIZE as f64) * 100.0
    }
    pub fn next_id(&self) -> u64 {
        self.next_id
    }
}

impl Drop for SegLog {
    fn drop(&mut self) {
        for &ptr in &self.segs {
            if !ptr.is_null() {
                unsafe {
                    dealloc(ptr, self.seg_layout);
                }
            }
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum LogError {
    EntryTooLarge,
    SegmentLimitReached,
    EntryNotFound,
    AllocationFailed,
    MaxSegmentsReached,
    WriteLockUnavailable,
}

impl std::fmt::Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogError::EntryTooLarge => write!(f, "entry exceeds segment capacity"),
            LogError::SegmentLimitReached => write!(f, "max segment count reached"),
            LogError::EntryNotFound => write!(f, "entry not found"),
            LogError::AllocationFailed => write!(f, "failed to allocate memory for segment"),
            LogError::MaxSegmentsReached => write!(f, "maximum segments reached"),
            LogError::WriteLockUnavailable => {
                write!(f, "write lock is currently held by another operation")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_full_lifecycle() {
        let mut log = SegLog::new();
        let entry_count = 1000u64;

        for i in 0..entry_count {
            let ts = 1700000000000 + i * 50;
            let payload = format!(
                "sensor:temp-{} value:{}.{} unit:c",
                i % 10,
                20 + (i % 15),
                i % 100
            );
            log.append(ts, payload.as_bytes()).await.unwrap();
        }
        assert_eq!(log.total_entries(), entry_count);

        for &id in &[1u64, 50, 250, 500, 750, 999, 1000] {
            let (read_id, read_ts, data) = log.read(id).unwrap();
            assert_eq!(read_id, id);
            let i = id - 1;
            assert_eq!(read_ts, 1700000000000 + i * 50);
            let expected = format!(
                "sensor:temp-{} value:{}.{} unit:c",
                i % 10,
                20 + (i % 15),
                i % 100
            );
            assert_eq!(data, expected.as_bytes());
        }

        let range = log.read_range(100, 109);
        assert_eq!(range.len(), 10);
        for (idx, (id, _, _)) in range.iter().enumerate() {
            assert_eq!(*id, 100 + idx as u64);
        }

        log.trim(501);
        assert_eq!(log.total_entries(), 500);
        assert!(log.read(1).is_err());
        assert!(log.read(500).is_err());
        assert!(log.read(501).is_ok());

        let new_id = log.append(9999999999999, b"post-trim-event").await.unwrap();
        assert_eq!(new_id, entry_count + 1);
        let (_, _, data) = log.read(new_id).unwrap();
        assert_eq!(data, b"post-trim-event");
    }

    #[tokio::test]
    async fn test_segment_boundary_integrity() {
        let mut log = SegLog::new();
        let payload_size = 1000;
        let count = 500u64;

        for i in 0..count {
            let fill_byte = (i & 0xFF) as u8;
            let payload = vec![fill_byte; payload_size];
            log.append(i, &payload).await.unwrap();
        }

        assert!(log.total_segments() > 1);

        for id in 1..=count {
            let (_, ts, data) = log.read(id).unwrap();
            let i = id - 1;
            assert_eq!(ts, i);
            assert_eq!(data.len(), payload_size);
            let expected_byte = (i & 0xFF) as u8;
            assert!(data.iter().all(|&b| b == expected_byte));
        }
    }

    #[tokio::test]
    async fn test_wire_format_and_zero_copy() {
        let mut log = SegLog::new();
        let ts: u64 = 0xAABB_CCDD_1122_3344;
        let payload = b"wire-fmt";
        let id = log.append(ts, payload).await.unwrap();

        let raw = log.get_raw_entry_slice(id).unwrap();
        assert_eq!(raw.len(), HEADER_SIZE + payload.len());

        let raw_id = u64::from_le_bytes(raw[0..8].try_into().unwrap());
        let raw_ts = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        let raw_len = u32::from_le_bytes(raw[16..20].try_into().unwrap()) as usize;
        let raw_payload = &raw[HEADER_SIZE..HEADER_SIZE + raw_len];

        let (s_id, s_ts, s_payload) = log.read(id).unwrap();
        assert_eq!(raw_id, s_id);
        assert_eq!(raw_ts, s_ts);
        assert_eq!(raw_ts, ts);
        assert_eq!(raw_payload, s_payload.as_slice());
        assert_eq!(raw_payload, payload);
    }

    #[tokio::test]
    async fn test_edge_cases() {
        let mut log = SegLog::new();

        let id1 = log.append(0, b"").await.unwrap();
        let (_, _, d) = log.read(id1).unwrap();
        assert!(d.is_empty());

        let max_payload = vec![0xFFu8; SEGMENT_SIZE - HEADER_SIZE];
        let id2 = log.append(u64::MAX, &max_payload).await.unwrap();
        let (_, ts, d) = log.read(id2).unwrap();
        assert_eq!(ts, u64::MAX);
        assert_eq!(d.len(), SEGMENT_SIZE - HEADER_SIZE);

        let oversized = vec![0u8; SEGMENT_SIZE - HEADER_SIZE + 1];
        assert!(log.append(1, &oversized).await.is_err());

        assert!(log.read(0).is_err());
        assert!(log.read(9999).is_err());
        assert!(log.get_raw_entry_slice(9999).is_err());
        assert!(log.read_range(5000, 6000).is_empty());
    }

    #[tokio::test]
    async fn test_drop_safety() {
        drop(SegLog::new());

        let mut log = SegLog::new();
        for _ in 0..300 {
            log.append(1, &vec![0u8; 2000]).await.unwrap();
        }
        log.trim(200);
        drop(log);
    }
}

#[cfg(test)]
mod bench {
    use super::*;
    use std::time::Instant;

    fn xorshift64(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    #[tokio::test]
    async fn bench_seg_log() {
        const TOTAL_OPS: u64 = 500_000;
        const NUM_KEYS: u64 = 50;
        const SMALL_PAYLOAD: usize = 64;
        const LARGE_PAYLOAD: usize = 4096;

        let mut rng: u64 = 0xDEAD_BEEF_CAFE_1234;
        let mut log = SegLog::new();

        let payload = vec![0xABu8; SMALL_PAYLOAD];
        let t = Instant::now();
        for i in 0..TOTAL_OPS / 2 {
            let ts = 1_700_000_000_000 + i * 10;
            log.append(ts, &payload).await.unwrap();
        }
        let half = TOTAL_OPS / 2;
        let elapsed = t.elapsed();
        println!(
            "[append:small] {} ops in {:.2}ms => {:.0} ops/sec | segments: {}",
            half,
            elapsed.as_secs_f64() * 1000.0,
            half as f64 / elapsed.as_secs_f64(),
            log.total_segments()
        );

        let large_payload = vec![0xCDu8; LARGE_PAYLOAD];
        let t = Instant::now();
        let mut large_ids = Vec::new();
        for i in 0..1000u64 {
            let id = log.append(9_000_000 + i, &large_payload).await.unwrap();
            large_ids.push(id);
        }
        let elapsed = t.elapsed();
        println!(
            "[append:large] 1000 ops ({} KB each) in {:.2}ms => {:.0} ops/sec | segments: {}",
            LARGE_PAYLOAD / 1024,
            elapsed.as_secs_f64() * 1000.0,
            1000.0 / elapsed.as_secs_f64(),
            log.total_segments()
        );

        let max_id = log.next_id() - 1;
        let t = Instant::now();
        let mut hits = 0u64;
        for _ in 0..100_000 {
            let id = (xorshift64(&mut rng) % max_id) + 1;
            if log.read(id).is_ok() {
                hits += 1;
            }
        }
        let elapsed = t.elapsed();
        println!(
            "[read:random] 100k reads in {:.2}ms => {:.0} ops/sec | hits: {}",
            elapsed.as_secs_f64() * 1000.0,
            100_000.0 / elapsed.as_secs_f64(),
            hits
        );

        for (start, end) in [(1, 100), (100, 500), (1000, 2000), (5000, 5999)] {
            let t = Instant::now();
            let results = log.read_range(start, end);
            let elapsed = t.elapsed();
            println!(
                "[read_range] id {}..={} => {} entries in {:.3}ms",
                start,
                end,
                results.len(),
                elapsed.as_secs_f64() * 1000.0
            );
        }

        let mut key_last_id: Vec<u64> = vec![0; NUM_KEYS as usize];
        let t = Instant::now();
        for _ in 0..TOTAL_OPS / 2 {
            let key = xorshift64(&mut rng) % NUM_KEYS;
            let ts = xorshift64(&mut rng);
            let size = (xorshift64(&mut rng) % 512 + 32) as usize;
            let payload = vec![(key & 0xFF) as u8; size];
            let id = log.append(ts, &payload).await.unwrap();
            key_last_id[key as usize] = id;
        }
        let elapsed = t.elapsed();
        let ops = TOTAL_OPS / 2;
        println!(
            "[append:multi-key] {} ops across {} keys in {:.2}ms => {:.0} ops/sec | segments: {}",
            ops,
            NUM_KEYS,
            elapsed.as_secs_f64() * 1000.0,
            ops as f64 / elapsed.as_secs_f64(),
            log.total_segments()
        );

        let mut verified = 0;
        for (key, &id) in key_last_id.iter().enumerate() {
            if id == 0 {
                continue;
            }
            let (read_id, _, data) = log.read(id).unwrap();
            assert_eq!(read_id, id);
            assert_eq!(data[0], (key & 0xFF) as u8);
            verified += 1;
        }
        println!("[verify] {}/{} keys verified", verified, NUM_KEYS);

        let cutoff = max_id / 2;
        let before = log.total_entries();
        let t = Instant::now();
        let trimmed = log.trim(cutoff);
        let elapsed = t.elapsed();
        let after = log.total_entries();
        println!(
            "[trim] cutoff={} | removed {} entries in {:.3}ms | entries: {} -> {}",
            cutoff,
            trimmed.len(),
            elapsed.as_secs_f64() * 1000.0,
            before,
            after
        );

        assert!(log.read(1).is_err());
        assert!(log.read(cutoff).is_ok());

        let pre_trim_next = log.next_id();
        let id_post = log.append(u64::MAX, b"post-trim").await.unwrap();
        assert_eq!(id_post, pre_trim_next);
        let (_, ts, data) = log.read(id_post).unwrap();
        assert_eq!(ts, u64::MAX);
        assert_eq!(data, b"post-trim");
        println!("[post-trim-append] id={} ok", id_post);

        println!(
            "[final] segments: {} | entries: {} | next_id: {}",
            log.total_segments(),
            log.total_entries(),
            log.next_id()
        );
    }
}
