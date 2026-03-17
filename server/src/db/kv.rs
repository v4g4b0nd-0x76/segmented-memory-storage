use std::collections::HashMap;

use anyhow::{Ok, anyhow};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DBEntry {
    pub val: Vec<u8>,
    pub ttl: i64,
    pub used_count: u64,
    pub last_used: i64,
}

#[derive(Clone, Debug)]
struct DB {
    id: String,
    entries: HashMap<String, DBEntry>,
}

impl DB {
    fn new(id: &str) -> Self {
        DB {
            id: id.to_string(),
            entries: HashMap::with_capacity(1024),
        }
    }

    fn set(&mut self, key: String, entry: DBEntry) {
        self.entries.insert(key, entry);
    }

    fn get(&mut self, key: &str) -> Option<DBEntry> {
        let expired = match self.entries.get(key) {
            None => return None,
            Some(val) => val.ttl != 0 && val.ttl < chrono::Utc::now().timestamp_millis(),
        };
        if expired {
            self.entries.remove(key);
            return None;
        }
        if let Some(entry) = self.entries.get_mut(key) {
            entry.used_count += 1;
            entry.last_used = chrono::Utc::now().timestamp_millis();
        }
        self.entries.get(key).cloned()
    }

    fn del(&mut self, key: &str) -> anyhow::Result<()> {
        self.entries
            .remove(key)
            .ok_or_else(|| anyhow!("{} not found", key))?;
        Ok(())
    }

    fn keys(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }

    fn flush(&mut self) {
        self.entries.clear();
    }
    fn expire_least_used(&mut self) {
        let sample_size = (self.entries.len() / 5).max(16).min(self.entries.len());
        if sample_size == 0 {
            return;
        }

        let mut candidates: Vec<(String, u64, i64)> = self
            .entries
            .iter()
            .take(sample_size)
            .map(|(k, v)| (k.clone(), v.used_count, v.last_used))
            .collect();

        candidates.sort_by(|a, b| a.1.cmp(&b.1).then(a.2.cmp(&b.2)));

        let remove_count = (sample_size / 4).max(1);
        for (key, _, _) in candidates.iter().take(remove_count) {
            self.entries.remove(key);
        }
    }
}

pub struct KvStore {
    dbs: HashMap<String, DB>,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore {
            dbs: HashMap::new(),
        }
    }

    fn get_db_mut(&mut self, db: &str) -> &mut DB {
        self.dbs
            .entry(db.to_string())
            .or_insert_with(|| DB::new(db))
    }

    pub fn set(&mut self, db: &str, key: String, val: Vec<u8>, ttl: i64) -> anyhow::Result<()> {
        let ttl_ts = if ttl == 0 {
            0
        } else {
            chrono::Utc::now().timestamp_millis() + ttl
        };

        let db = self.get_db_mut(db);
        if db.entries.len() >= 1024 {
            let _ = db.expire_least_used();
        }
        db.set(
            key,
            DBEntry {
                val,
                ttl: ttl_ts,
                used_count: 0,
                last_used: chrono::Utc::now().timestamp_millis(),
            },
        );
        Ok(())
    }

    pub fn get(&mut self, db: &str, key: String) -> anyhow::Result<Option<DBEntry>> {
        let result = self.get_db_mut(db).get(&key);
        Ok(result)
    }

    pub fn del(&mut self, db: &str, key: &str) -> anyhow::Result<()> {
        self.get_db_mut(db).del(key)
    }

    pub fn keys(&self, db: &str) -> anyhow::Result<Vec<String>> {
        match self.dbs.get(db) {
            None => Ok(vec![]),
            Some(db) => Ok(db.keys()),
        }
    }

    pub fn flush(&mut self, db: &str) -> anyhow::Result<()> {
        self.get_db_mut(db).flush();

        Ok(())
    }

    pub fn create_db(&mut self, id: &str) -> anyhow::Result<()> {
        if self.dbs.contains_key(id) {
            return Err(anyhow!("database {} already exists", id));
        }
        self.dbs.insert(id.to_string(), DB::new(id));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_cycle() {
        let mut store = KvStore::new();

        store
            .set("db", "k1".to_string(), b"hello".to_vec(), 0)
            .unwrap();
        store
            .set("db", "k2".to_string(), b"world".to_vec(), 0)
            .unwrap();

        let v1 = store.get("db", "k1".to_string()).unwrap().unwrap();
        assert_eq!(v1.val, b"hello");

        let keys = store.keys("db").unwrap();
        assert_eq!(keys.len(), 2);

        store.del("db", "k1").unwrap();
        assert!(store.get("db", "k1".to_string()).unwrap().is_none());

        store.flush("db").unwrap();
        assert!(store.keys("db").unwrap().is_empty());
    }

    #[test]
    fn test_ttl_expiry() {
        let mut store = KvStore::new();
        store.set("db", "k1".to_string(), b"v".to_vec(), 1).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(store.get("db", "k1".to_string()).unwrap().is_none());
    }

    #[test]
    fn test_create_db_duplicate() {
        let mut store = KvStore::new();
        store.create_db("mydb").unwrap();
        assert!(store.create_db("mydb").is_err());
    }
}

#[cfg(test)]
mod bench {
    use super::*;
    use std::time::Instant;

    #[test]
    fn bench_kv_realistic() {
        let total: usize = 5_000_000;
        let unique_keys: usize = 2_000_000;
        let db = "bench_db";

        let mut store = KvStore::new();

        println!(
            "\n[BENCH] Writing {} entries ({} unique keys)...",
            total, unique_keys
        );
        let start = Instant::now();

        for i in 0..total {
            let key = format!("key:{}", i % unique_keys);
            let val = format!("value:{}", i).into_bytes();
            store.set(db, key, val, 0).unwrap();
        }

        let elapsed = start.elapsed();
        println!(
            "[BENCH] Write done: {:?} | {:.0} ops/sec",
            elapsed,
            total as f64 / elapsed.as_secs_f64()
        );

        let delete_count = unique_keys / 10;
        println!("[BENCH] Deleting {} keys...", delete_count);
        let start = Instant::now();

        for i in 0..delete_count {
            let _ = store.del(db, &format!("key:{}", i));
        }

        let elapsed = start.elapsed();
        println!(
            "[BENCH] Delete done: {:?} | {:.0} ops/sec",
            elapsed,
            delete_count as f64 / elapsed.as_secs_f64()
        );

        let read_count = 500_000;
        println!(
            "[BENCH] Reading {} keys (mixed hits/misses/deleted)...",
            read_count
        );
        let start = Instant::now();
        let mut hits = 0usize;
        let mut misses = 0usize;

        for i in 0..read_count {
            let key = format!("key:{}", i % unique_keys);
            match store.get(db, key).unwrap() {
                Some(_) => hits += 1,
                None => misses += 1,
            }
        }

        let elapsed = start.elapsed();
        println!(
            "[BENCH] Read done: {:?} | {:.0} ops/sec | hits={} misses={}",
            elapsed,
            read_count as f64 / elapsed.as_secs_f64(),
            hits,
            misses
        );

        println!("[BENCH] Flushing db...");
        let start = Instant::now();
        store.flush(db).unwrap();
        println!("[BENCH] Flush done: {:?}", start.elapsed());

        assert!(store.keys(db).unwrap().is_empty());
    }
}
