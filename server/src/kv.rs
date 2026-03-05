use std::{collections::HashMap, ops::Add, path::PathBuf};

use anyhow::{Ok, anyhow};
use futures::task::ArcWake;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::fs;
const AOF_DIR: &str = "aof";

// a kv store with aof
#[derive(Serialize, Deserialize, Clone, Debug)] // here json is certainly is not cheap either in parsing neither while saving but i chose json for simplicity
pub struct DBEntry {
    pub val: Vec<u8>,
    pub ttl: i64, // we store the timestamp of which this entry should be expired so when replicate this db over other instances or saving in aof we know exact time that this shall be expired
                  // 0 for ttl means this entry lives for ever
}
#[derive(Serialize, Deserialize)]
enum Command {
    SET(String),
    DEL(String),
}
#[derive(Serialize, Deserialize)]
struct AofEntry {
    command: Command,
    key: String,
    entry: Option<DBEntry>,
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

    async fn set(&mut self, key: String, entry: DBEntry) -> anyhow::Result<()> {
        // ttl in here is pre calculated in tcp server as the dto for insert is different
        self.entries.insert(key.clone(), entry.clone());
        self.append_aof(Command::SET("SET".to_string()), key, Some(entry))
            .await?;
        Ok(())
    }
    async fn get(&mut self, key: String) -> anyhow::Result<Option<&DBEntry>, anyhow::Error> {
        self.entries
            .get(&key)
            .map(|val| {
                if val.ttl > chrono::Utc::now().timestamp_millis() {
                    self.entries.to_owned().remove(&key);
                    return None;
                }
                return Some(val);
            })
            .ok_or(anyhow::anyhow!("{} not found", key))
    }
    async fn del(&mut self, key: String) -> anyhow::Result<(), anyhow::Error> {
        let _ = self
            .entries
            .remove(&key)
            .ok_or(anyhow::anyhow!("{} not found", key));
        self.append_aof(Command::DEL("DEL".to_string()), key, None)
            .await?;
        Ok(())
    }
    async fn keys(&self) -> anyhow::Result<Vec<&String>> {
        let keys: Vec<&String> = self.entries.keys().collect();
        Ok(keys)
    }
    async fn flush(&mut self) -> anyhow::Result<(), anyhow::Error> {
        self.entries = HashMap::new();
        self.flush_aof().await?;
        Ok(())
    }
    async fn append_aof(
        &self,
        cmd: Command,
        key: String,
        entry: Option<DBEntry>,
    ) -> anyhow::Result<()> {
        // TODO: add new entries to some vec and after a certain limit flush it into the file
        let _ = fs::create_dir_all(AOF_DIR).await;
        let path: PathBuf = PathBuf::from(format!("{}/{}.aof", AOF_DIR, self.id));
        let content = fs::read_to_string(&path).await?;
        let mut entries = serde_json::from_str::<Vec<AofEntry>>(&content)?;
        entries.push(AofEntry {
            command: cmd,
            key,
            entry,
        });
        let new_content = serde_json::json!(entries);
        fs::write(&path, new_content.to_string().as_bytes()).await?;
        Ok(())
    }
    async fn flush_aof(&mut self) -> anyhow::Result<(), anyhow::Error> {
        let _ = fs::create_dir_all(AOF_DIR).await;
        let path = PathBuf::from(format!("{}/{}.aof", AOF_DIR, self.id));
        fs::write(path, []).await?;
        Ok(())
    }
}
pub struct KvStore {
    dbs: HashMap<String, DB>,
}

impl KvStore {
    pub async fn new() -> Self {
        let dbs: HashMap<String, DB> = load_dbs()
            .await
            .map_err(|e| {
                println!("failed to load dbs {:#?}", e);
                let map: HashMap<String, DB> = HashMap::new();
                return map;
            })
            .unwrap();

        KvStore { dbs: dbs }
    }
    pub async fn set(
        &mut self,
        db: String,
        key: String,
        val: Vec<u8>,
        ttl: i64,
    ) -> anyhow::Result<()> {
        let db = self
            .dbs
            .get_mut(&db)
            .ok_or(anyhow!("invalid database"))
            .unwrap();
        let ttl_ts = chrono::Utc::now().timestamp_millis() + ttl;
        db.set(
            key,
            DBEntry {
                val: val,
                ttl: ttl_ts,
            },
        )
        .await?;
        Ok(())
    }
    pub async fn get(
        &mut self,
        db: String,
        key: String,
    ) -> anyhow::Result<Option<&DBEntry>, anyhow::Error> {
        let db = self.dbs.get_mut(&db);
        if let Some(db) = db {
            db.get(key).await?;
        }
        return Ok(None);
    }
    pub async fn del(&mut self, db: String, key: String) -> anyhow::Result<(), anyhow::Error> {
        let db = self.dbs.get_mut(&db);
        if let Some(db) = db {
            db.del(key).await?;
        }
        return Ok(());
    }
    pub async fn keys(&mut self, db: String) -> anyhow::Result<Vec<&String>> {
        let db = self
            .dbs
            .get(&db)
            .ok_or(anyhow!("invalid database"))
            .unwrap();
        let res = db.keys().await?;
        Ok(res)
    }
    pub async fn flush(&mut self, db: String) -> anyhow::Result<(), anyhow::Error> {
        let db = self
            .dbs
            .get_mut(&db)
            .ok_or(anyhow!("invalid database"))
            .unwrap();
        db.flush().await?;
        Ok(())
    }
}
async fn load_dbs() -> anyhow::Result<HashMap<String, DB>> {
    let _ = fs::create_dir_all(AOF_DIR).await;
    let mut aof_dir = fs::read_dir(AOF_DIR).await?;
    let mut dbs: HashMap<String, DB> = HashMap::new();
    while let Some(entry) = aof_dir.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            continue;
        }
        let file_name = entry.file_name();
        if !file_name.to_str().unwrap().contains(".aof") {
            continue;
        };

        let parts: Vec<&str> = file_name.to_str().unwrap().split(".aof").collect();
        let db_num = parts[0];
        let mut db = DB::new(db_num.to_string());
        let mut insert_commands: HashMap<String, DBEntry> = HashMap::new();
        let mut del_keys: Vec<String> = Vec::new();
        // at end we just apply the insert commands after remove keys in del_commands from it
        let aof = fs::read_to_string(PathBuf::from(format!(
            "aof/{}",
            file_name.to_string_lossy()
        )))
        .await?;
        let entries = serde_json::from_str::<Vec<AofEntry>>(&aof)?;
        for entry in entries {
            match entry.command {
                Command::SET(_) => {
                    if let Some(db_entry) = entry.entry {
                        let e = db_entry.clone();
                        insert_commands.insert(entry.key, e);
                    }
                }
                Command::DEL(_) => del_keys.push(entry.key),
            }
        }
        for (k, v) in insert_commands {
            if !del_keys.contains(&k) {
                db.set(k, v).await?;
            }
        }
        dbs.insert(db_num.to_string(), db);
    }
    Ok(dbs)
}
