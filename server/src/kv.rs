use std::{collections::HashMap, path::PathBuf};

use anyhow::{Ok, anyhow};
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::AsyncWriteExt,
    sync::mpsc::{self, UnboundedSender},
};

const AOF_DIR: &str = "aof";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DBEntry {
    pub val: Vec<u8>,
    pub ttl: i64,
}

#[derive(Serialize, Deserialize)]
enum Command {
    SET,
    DEL,
}

#[derive(Serialize, Deserialize)]
struct AofEntry {
    command: Command,
    key: String,
    entry: Option<DBEntry>,
}

enum AofMsg {
    Append(AofEntry),
    Flush(tokio::sync::oneshot::Sender<()>),
    Shutdown(tokio::sync::oneshot::Sender<()>),
}

fn spawn_aof_worker(db_id: String) -> UnboundedSender<AofMsg> {
    let (tx, mut rx) = mpsc::unbounded_channel::<AofMsg>();

    tokio::spawn(async move {
        let path = PathBuf::from(format!("{}/{}.aof", AOF_DIR, db_id));
        fs::create_dir_all(AOF_DIR).await.ok();

        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .expect("failed to open aof file");

        while let Some(msg) = rx.recv().await {
            match msg {
                AofMsg::Append(entry) => {
                    if let std::result::Result::Ok(mut line) = serde_json::to_vec(&entry) {
                        line.push(b'\n');
                        file.write_all(&line).await.ok();
                    }
                }
                AofMsg::Flush(ack) => {
                    drop(file);
                    fs::write(&path, b"").await.ok();
                    file = fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&path)
                        .await
                        .expect("failed to reopen aof file");
                    let _ = ack.send(());
                }
                AofMsg::Shutdown(ack) => {
                    file.flush().await.ok();
                    let _ = ack.send(());
                    break;
                }
            }
        }
    });

    tx
}

#[derive(Clone, Debug)]
struct DB {
    id: String,
    entries: HashMap<String, DBEntry>,
    aof_tx: UnboundedSender<AofMsg>,
}

impl DB {
    fn new(id: String) -> Self {
        let aof_tx = spawn_aof_worker(id.clone());
        DB {
            id,
            entries: HashMap::new(),
            aof_tx,
        }
    }

    fn with_entries(id: String, entries: HashMap<String, DBEntry>) -> Self {
        let aof_tx = spawn_aof_worker(id.clone());
        DB {
            id,
            entries,
            aof_tx,
        }
    }

    fn set(&mut self, key: String, entry: DBEntry) -> anyhow::Result<()> {
        self.entries.insert(key.clone(), entry.clone());
        self.aof_tx.send(AofMsg::Append(AofEntry {
            command: Command::SET,
            key,
            entry: Some(entry),
        }))?;
        Ok(())
    }

    fn get(&mut self, key: String) -> anyhow::Result<Option<DBEntry>> {
        match self.entries.get(&key) {
            None => Ok(None),
            Some(val) => {
                if val.ttl != 0 && val.ttl < chrono::Utc::now().timestamp_millis() {
                    self.entries.remove(&key);
                    self.aof_tx.send(AofMsg::Append(AofEntry {
                        command: Command::DEL,
                        key,
                        entry: None,
                    }))?;
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
        self.aof_tx.send(AofMsg::Append(AofEntry {
            command: Command::DEL,
            key,
            entry: None,
        }))?;
        Ok(())
    }

    fn keys(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.entries.clear();
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.aof_tx.send(AofMsg::Flush(ack_tx))?;
        ack_rx.await?;
        Ok(())
    }
    async fn shutdown(&mut self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.aof_tx.send(AofMsg::Shutdown(tx));
        let _ = rx.await;
    }
}

pub struct KvStore {
    dbs: HashMap<String, DB>,
}

impl KvStore {
    pub async fn new() -> Self {
        let dbs = load_dbs().await.unwrap_or_else(|e| {
            eprintln!("failed to load dbs: {:#?}", e);
            HashMap::new()
        });
        KvStore { dbs }
    }

    async fn get_db_mut(&mut self, db: &str) -> anyhow::Result<&mut DB> {
        if !self.dbs.contains_key(db) {
            self.create_db(db.to_string()).await?;
        }
        self.dbs
            .get_mut(db)
            .ok_or_else(|| anyhow!("invalid database: {}", db))
    }

    async fn get_db(&mut self, db: &str) -> anyhow::Result<&DB> {
        if !self.dbs.contains_key(db) {
            self.create_db(db.to_string()).await?;
        }
        self.dbs
            .get(db)
            .ok_or_else(|| anyhow!("invalid database: {}", db))
    }

    pub async fn set(
        &mut self,
        db: String,
        key: String,
        val: Vec<u8>,
        ttl: i64,
    ) -> anyhow::Result<()> {
        let ttl_ts = if ttl == 0 {
            0
        } else {
            chrono::Utc::now().timestamp_millis() + ttl
        };
        let db = self.get_db_mut(&db).await?;
        db.set(key, DBEntry { val, ttl: ttl_ts })
    }

    pub async fn get(&mut self, db: String, key: String) -> anyhow::Result<Option<DBEntry>> {
        let db = self.get_db_mut(&db).await?;
        db.get(key)
    }

    pub async fn del(&mut self, db: String, key: String) -> anyhow::Result<()> {
        let db = self.get_db_mut(&db).await?;
        db.del(key)
    }

    pub async fn keys(&mut self, db: String) -> anyhow::Result<Vec<String>> {
        let db = self.get_db(&db).await?;
        Ok(db.keys())
    }

    pub async fn flush(&mut self, db: String) -> anyhow::Result<()> {
        let db = self.get_db_mut(&db).await?;
        db.flush().await
    }

    pub async fn create_db(&mut self, id: String) -> anyhow::Result<()> {
        if self.dbs.contains_key(&id) {
            return Err(anyhow!("database {} already exists", id));
        }
        self.dbs.insert(id.clone(), DB::new(id));
        Ok(())
    }
    pub async fn shutdown(&mut self) {
        for db in self.dbs.values_mut() {
            db.shutdown().await;
        }
    }
}
async fn load_dbs() -> anyhow::Result<HashMap<String, DB>> {
    fs::create_dir_all(AOF_DIR).await?;
    let mut aof_dir = fs::read_dir(AOF_DIR).await?;
    let mut tasks = tokio::task::JoinSet::new();

    while let Some(entry) = aof_dir.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            continue;
        }
        let file_name = entry.file_name();
        let file_str = file_name.to_string_lossy();
        if !file_str.ends_with(".aof") {
            continue;
        }
        let db_id = file_str.trim_end_matches(".aof").to_string();
        let path = entry.path();

        tasks.spawn(tokio::task::spawn_blocking(
            move || -> (String, HashMap<String, DBEntry>) {
                let aof = std::fs::read_to_string(&path).unwrap_or_default();
                let mut entries: HashMap<String, DBEntry> = HashMap::new();

                for line in aof.lines() {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    if let std::result::Result::Ok(aof_entry) =
                        serde_json::from_str::<AofEntry>(line)
                    {
                        match aof_entry.command {
                            Command::SET => {
                                if let Some(db_entry) = aof_entry.entry {
                                    entries.insert(aof_entry.key, db_entry);
                                }
                            }
                            Command::DEL => {
                                entries.remove(&aof_entry.key);
                            }
                        }
                    }
                }

                (db_id, entries)
            },
        ));
    }

    let mut dbs: HashMap<String, DB> = HashMap::new();
    while let Some(res) = tasks.join_next().await {
        let (db_id, entries) = res??;
        dbs.insert(db_id.clone(), DB::with_entries(db_id, entries));
    }

    Ok(dbs)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn cleanup(db_id: &str) {
        let path = format!("{}/{}.aof", AOF_DIR, db_id);
        let _ = fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_full_cycle() {
        let db_id = "test_cycle";
        cleanup(db_id).await;

        let mut store = KvStore::new().await;
        store
            .set(db_id.to_string(), "k1".to_string(), b"hello".to_vec(), 0)
            .await
            .unwrap();
        store
            .set(db_id.to_string(), "k2".to_string(), b"world".to_vec(), 0)
            .await
            .unwrap();

        let v1 = store
            .get(db_id.to_string(), "k1".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v1.val, b"hello");

        let keys = store.keys(db_id.to_string()).await.unwrap();
        assert_eq!(keys.len(), 2);

        store
            .del(db_id.to_string(), "k1".to_string())
            .await
            .unwrap();
        assert!(
            store
                .get(db_id.to_string(), "k1".to_string())
                .await
                .unwrap()
                .is_none()
        );

        store.flush(db_id.to_string()).await.unwrap();
        assert!(store.keys(db_id.to_string()).await.unwrap().is_empty());

        cleanup(db_id).await;
    }

    #[tokio::test]
    async fn test_aof_persistence() {
        let db_id = "test_aof_persistence_isolated";
        cleanup(db_id).await;

        {
            let mut store = KvStore::new().await;
            store
                .set(db_id.to_string(), "key1".to_string(), b"val1".to_vec(), 0)
                .await
                .unwrap();
            store
                .set(db_id.to_string(), "key2".to_string(), b"val2".to_vec(), 0)
                .await
                .unwrap();
            store
                .del(db_id.to_string(), "key2".to_string())
                .await
                .unwrap();
            store.shutdown().await;
        }

        let mut store2 = KvStore::new().await;
        let v1 = store2
            .get(db_id.to_string(), "key1".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v1.val, b"val1");

        let v2 = store2
            .get(db_id.to_string(), "key2".to_string())
            .await
            .unwrap();
        assert!(v2.is_none());

        cleanup(db_id).await;
    }
}

#[cfg(test)]
mod bench {
    use super::*;
    use std::time::Instant;

    async fn cleanup_bench() {
        let _ = fs::remove_file(format!("{}/bench_db.aof", AOF_DIR)).await;
    }

    #[tokio::test]
    async fn bench_kv_realistic() {
        cleanup_bench().await;

        let total: usize = 1_000_000;
        let unique_keys: usize = 200_000;
        let db = "bench_db".to_string();

        let mut store = KvStore::new().await;

        // --- Phase 1: Write 1M keys (with duplicates) ---
        println!(
            "\n[BENCH] Writing {} entries ({} unique keys)...",
            total, unique_keys
        );
        let start = Instant::now();

        for i in 0..total {
            let key = format!("key:{}", i % unique_keys);
            let val = format!("value:{}", i).into_bytes();
            store.set(db.clone(), key, val, 0).await.unwrap();
        }

        let write_elapsed = start.elapsed();
        println!(
            "[BENCH] Write done: {:?} | {:.0} ops/sec",
            write_elapsed,
            total as f64 / write_elapsed.as_secs_f64()
        );

        // --- Phase 2: Delete 10% of unique keys ---
        let delete_count = unique_keys / 10;
        println!("[BENCH] Deleting {} keys...", delete_count);
        let start = Instant::now();

        for i in 0..delete_count {
            let key = format!("key:{}", i);
            let _ = store.del(db.clone(), key).await;
        }

        let delete_elapsed = start.elapsed();
        println!(
            "[BENCH] Delete done: {:?} | {:.0} ops/sec",
            delete_elapsed,
            delete_count as f64 / delete_elapsed.as_secs_f64()
        );

        // --- Phase 3: Mixed reads (hits, misses, deleted) ---
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
            match store.get(db.clone(), key).await.unwrap() {
                Some(_) => hits += 1,
                None => misses += 1,
            }
        }

        let read_elapsed = start.elapsed();
        println!(
            "[BENCH] Read done: {:?} | {:.0} ops/sec | hits={} misses={}",
            read_elapsed,
            read_count as f64 / read_elapsed.as_secs_f64(),
            hits,
            misses
        );

        // --- Phase 4: AOF reload simulation ---
        println!("[BENCH] Simulating restart (AOF reload)...");
        store.shutdown().await;
        drop(store);
        let start = Instant::now();
        let mut store2 = KvStore::new().await;
        let reload_elapsed = start.elapsed();
        println!("[BENCH] AOF reload done: {:?}", reload_elapsed);

        // --- Phase 5: Post-reload read verification ---
        let verify_count = 1000;
        println!("[BENCH] Verifying {} keys after reload...", verify_count);
        let mut ok = 0usize;

        for i in delete_count..delete_count + verify_count {
            let key = format!("key:{}", i % unique_keys);
            if store2.get(db.clone(), key).await.unwrap().is_some() {
                ok += 1;
            }
        }
        println!("[BENCH] Post-reload hits: {}/{}", ok, verify_count);
        assert_eq!(ok, verify_count);

        // --- Phase 6: Flush ---
        println!("[BENCH] Flushing db...");
        let start = Instant::now();
        store2.flush(db.clone()).await.unwrap();
        println!("[BENCH] Flush done: {:?}", start.elapsed());

        assert!(store2.keys(db).await.unwrap().is_empty());

        cleanup_bench().await;
    }
}
