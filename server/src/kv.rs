use std::{collections::HashMap, path::PathBuf};

use anyhow::{Ok, anyhow};
use serde::{Deserialize, Serialize};
use tokio::fs;

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
        self.entries.insert(key.clone(), entry.clone());
        self.append_aof(Command::SET, key, Some(entry)).await?;
        Ok(())
    }

    async fn get(&mut self, key: String) -> anyhow::Result<Option<DBEntry>> {
        match self.entries.get(&key) {
            None => Ok(None),
            Some(val) => {
                if val.ttl != 0 && val.ttl < chrono::Utc::now().timestamp_millis() {
                    self.entries.remove(&key);
                    self.append_aof(Command::DEL, key, None).await?;
                    return Ok(None);
                }
                Ok(self.entries.get(&key).cloned())
            }
        }
    }

    async fn del(&mut self, key: String) -> anyhow::Result<()> {
        self.entries
            .remove(&key)
            .ok_or_else(|| anyhow!("{} not found", key))?;
        self.append_aof(Command::DEL, key, None).await?;
        Ok(())
    }

    fn keys(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.entries.clear();
        self.flush_aof().await?;
        Ok(())
    }

    async fn append_aof(
        &self,
        cmd: Command,
        key: String,
        entry: Option<DBEntry>,
    ) -> anyhow::Result<()> {
        fs::create_dir_all(AOF_DIR).await?;
        let path = PathBuf::from(format!("{}/{}.aof", AOF_DIR, self.id));

        let mut entries: Vec<AofEntry> = if path.exists() {
            let content = fs::read_to_string(&path).await?;
            if content.trim().is_empty() {
                vec![]
            } else {
                serde_json::from_str(&content)?
            }
        } else {
            vec![]
        };

        entries.push(AofEntry {
            command: cmd,
            key,
            entry,
        });
        fs::write(&path, serde_json::to_vec(&entries)?).await?;
        Ok(())
    }

    async fn flush_aof(&self) -> anyhow::Result<()> {
        fs::create_dir_all(AOF_DIR).await?;
        let path = PathBuf::from(format!("{}/{}.aof", AOF_DIR, self.id));
        fs::write(path, b"[]").await?;
        Ok(())
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
        db.set(key, DBEntry { val, ttl: ttl_ts }).await
    }

    pub async fn get(&mut self, db: String, key: String) -> anyhow::Result<Option<DBEntry>> {
        let db = self.get_db_mut(&db).await?;
        db.get(key).await
    }

    pub async fn del(&mut self, db: String, key: String) -> anyhow::Result<()> {
        let db = self.get_db_mut(&db).await?;
        db.del(key).await
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
}

async fn load_dbs() -> anyhow::Result<HashMap<String, DB>> {
    fs::create_dir_all(AOF_DIR).await?;
    let mut aof_dir = fs::read_dir(AOF_DIR).await?;
    let mut dbs: HashMap<String, DB> = HashMap::new();

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
        let mut db = DB::new(db_id.clone());

        let aof = fs::read_to_string(entry.path()).await?;
        if aof.trim().is_empty() {
            dbs.insert(db_id, db);
            continue;
        }

        let entries: Vec<AofEntry> = serde_json::from_str(&aof)?;

        let mut insert_commands: HashMap<String, DBEntry> = HashMap::new();
        let mut del_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

        for entry in entries {
            match entry.command {
                Command::SET => {
                    if let Some(db_entry) = entry.entry {
                        insert_commands.insert(entry.key, db_entry);
                    }
                }
                Command::DEL => {
                    del_keys.insert(entry.key);
                }
            }
        }

        for (k, v) in insert_commands {
            if !del_keys.contains(&k) {
                db.entries.insert(k, v);
            }
        }

        dbs.insert(db_id, db);
    }

    Ok(dbs)
}
