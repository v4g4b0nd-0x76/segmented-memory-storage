use std::{collections::HashMap, time::Duration};

use anyhow::{Ok, anyhow};
use serde::{Deserialize, Serialize};

use crate::db::lru::LRU;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DBEntry {
    pub val: Vec<u8>,
    pub ttl: i64,
}

#[derive(Clone, Debug)]
struct DB {
    id: String,
    entries: HashMap<String, DBEntry>,
}

impl DB {
    fn new(id: String) -> Self {
        DB {
            id,
            entries: HashMap::new(),
        }
    }

    fn set(&mut self, key: String, entry: DBEntry) -> anyhow::Result<()> {
        self.entries.insert(key, entry);
        Ok(())
    }

    fn get(&mut self, key: String) -> anyhow::Result<Option<DBEntry>> {
        match self.entries.get(&key) {
            None => Ok(None),
            Some(val) => {
                if val.ttl != 0 && val.ttl < chrono::Utc::now().timestamp_millis() {
                    self.entries.remove(&key);
                    return Ok(None);
                }
                Ok(self.entries.get(&key).cloned())
            }
        }
    }

    fn del(&mut self, key: String) -> anyhow::Result<()> {
        self.entries
            .remove(&key)
            .ok_or_else(|| anyhow!("{} not found", key))?;
        Ok(())
    }

    fn keys(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }

    fn flush(&mut self) {
        self.entries.clear();
    }
}

pub struct KvStore {
    dbs: HashMap<String, DB>,
    lru: LRU<String, DBEntry>,
    lru_size: usize,
}

impl KvStore {
    fn cache_key(db: &str, key: &str) -> String {
        format!("{}:{}", db, key)
    }

    pub fn new(lru_size: Option<usize>) -> Self {
        let lru_size = lru_size.unwrap_or(100_000);
        KvStore {
            dbs: HashMap::new(),
            lru: LRU::new(lru_size, Duration::from_secs(60)),
            lru_size,
        }
    }

    fn get_db_mut(&mut self, db: &str) -> anyhow::Result<&mut DB> {
        if !self.dbs.contains_key(db) {
            self.dbs.insert(db.to_string(), DB::new(db.to_string()));
        }
        self.dbs
            .get_mut(db)
            .ok_or_else(|| anyhow!("invalid database: {}", db))
    }

    fn get_db(&self, db: &str) -> anyhow::Result<&DB> {
        self.dbs
            .get(db)
            .ok_or_else(|| anyhow!("invalid database: {}", db))
    }

    pub fn set(&mut self, db: String, key: String, val: Vec<u8>, ttl: i64) -> anyhow::Result<()> {
        let ttl_ts = if ttl == 0 {
            0
        } else {
            chrono::Utc::now().timestamp_millis() + ttl
        };
        self.lru.remove(&Self::cache_key(&db, &key));
        let db = self.get_db_mut(&db)?;
        db.set(key, DBEntry { val, ttl: ttl_ts })
    }

    pub fn get(&mut self, db: String, key: String) -> anyhow::Result<Option<DBEntry>> {
        let ck = Self::cache_key(&db, &key);
        if let Some(entry) = self.lru.get(&ck) {
            return Ok(Some(entry.clone()));
        }
        let db_ref = self.get_db_mut(&db)?;
        let result = db_ref.get(key)?;
        if let Some(ref entry) = result {
            self.lru.insert(ck, entry.clone());
        }
        Ok(result)
    }

    pub fn del(&mut self, db: String, key: String) -> anyhow::Result<()> {
        self.lru.remove(&Self::cache_key(&db, &key));
        let db = self.get_db_mut(&db)?;
        db.del(key)
    }

    pub fn keys(&self, db: String) -> anyhow::Result<Vec<String>> {
        if !self.dbs.contains_key(&db) {
            return Ok(vec![]);
        }
        Ok(self.get_db(&db)?.keys())
    }

    pub fn flush(&mut self, db: String) -> anyhow::Result<()> {
        let db = self.get_db_mut(&db)?;
        db.flush();
        self.lru = LRU::new(self.lru_size, Duration::from_secs(60));
        Ok(())
    }

    pub fn create_db(&mut self, id: String) -> anyhow::Result<()> {
        if self.dbs.contains_key(&id) {
            return Err(anyhow!("database {} already exists", id));
        }
        self.dbs.insert(id.clone(), DB::new(id));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_cycle() {
        let mut store = KvStore::new(None);

        store
            .set("db".into(), "k1".into(), b"hello".to_vec(), 0)
            .unwrap();
        store
            .set("db".into(), "k2".into(), b"world".to_vec(), 0)
            .unwrap();

        let v1 = store.get("db".into(), "k1".into()).unwrap().unwrap();
        assert_eq!(v1.val, b"hello");

        let keys = store.keys("db".into()).unwrap();
        assert_eq!(keys.len(), 2);

        store.del("db".into(), "k1".into()).unwrap();
        assert!(store.get("db".into(), "k1".into()).unwrap().is_none());

        store.flush("db".into()).unwrap();
        assert!(store.keys("db".into()).unwrap().is_empty());
    }

    #[test]
    fn test_ttl_expiry() {
        let mut store = KvStore::new(None);
        store
            .set("db".into(), "k1".into(), b"v".to_vec(), 1)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(store.get("db".into(), "k1".into()).unwrap().is_none());
    }

    #[test]
    fn test_create_db_duplicate() {
        let mut store = KvStore::new(None);
        store.create_db("mydb".into()).unwrap();
        assert!(store.create_db("mydb".into()).is_err());
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
        let db = "bench_db".to_string();

        let mut store = KvStore::new(Some(unique_keys));

        // --- Phase 1: Write ---
        println!(
            "\n[BENCH] Writing {} entries ({} unique keys)...",
            total, unique_keys
        );
        let start = Instant::now();

        for i in 0..total {
            let key = format!("key:{}", i % unique_keys);
            let val = format!("value:{}", i).into_bytes();
            store.set(db.clone(), key, val, 0).unwrap();
        }

        let elapsed = start.elapsed();
        println!(
            "[BENCH] Write done: {:?} | {:.0} ops/sec",
            elapsed,
            total as f64 / elapsed.as_secs_f64()
        );

        // --- Phase 2: Delete 10% ---
        let delete_count = unique_keys / 10;
        println!("[BENCH] Deleting {} keys...", delete_count);
        let start = Instant::now();

        for i in 0..delete_count {
            let _ = store.del(db.clone(), format!("key:{}", i));
        }

        let elapsed = start.elapsed();
        println!(
            "[BENCH] Delete done: {:?} | {:.0} ops/sec",
            elapsed,
            delete_count as f64 / elapsed.as_secs_f64()
        );

        // --- Phase 3: Mixed reads ---
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
            match store.get(db.clone(), key).unwrap() {
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

        // --- Phase 4: Flush ---
        println!("[BENCH] Flushing db...");
        let start = Instant::now();
        store.flush(db.clone()).unwrap();
        println!("[BENCH] Flush done: {:?}", start.elapsed());

        assert!(store.keys(db).unwrap().is_empty());
    }
}
