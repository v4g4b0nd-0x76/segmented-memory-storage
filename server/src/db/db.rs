use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock};

use crate::db::{
    aof::{AofEntry, AofWriter, load_aof},
    groups::{GroupError, GroupManager, GroupStats},
    kv::{DBEntry, KvStore},
    pipeline::PipelineManager,
    seg_list::{ListError, SegList},
};

pub struct DB {
    group_manager: Arc<RwLock<GroupManager>>,
    kv_store: Arc<RwLock<KvStore>>,
    list: Arc<RwLock<SegList>>,
    aof: AofWriter,
    aof_path: PathBuf,
}

impl DB {
    pub async fn new() -> Self {
        let aof_path = PathBuf::from("aof/log.aof");
        let aof = AofWriter::new(aof_path.clone());
        let mut db = DB {
            group_manager: Arc::new(RwLock::new(GroupManager::new())),
            kv_store: Arc::new(RwLock::new(KvStore::new(None))),
            list: Arc::new(RwLock::new(SegList::new().await)),
            aof,
            aof_path,
        };

        db.load_and_apply().await.expect("failed to load aof");
        db
    }

    pub async fn load_and_apply(&mut self) -> anyhow::Result<()> {
        let entries = load_aof(&self.aof_path).await?;
        println!("[AOF] replaying {} entries...", entries.len());

        let kv_store = Arc::clone(&self.kv_store);
        let group_manager = Arc::clone(&self.group_manager);
        let list = Arc::clone(&self.list);

        let kv1 = Arc::clone(&kv_store);
        let gm1 = Arc::clone(&group_manager);
        let l1 = Arc::clone(&list);
        let h1 = tokio::spawn(async move {
            for entry in entries.kv_entries {
                apply_aof(&kv1, &l1, &gm1, entry).await;
            }
        });

        let kv2 = Arc::clone(&kv_store);
        let gm2 = Arc::clone(&group_manager);
        let l2 = Arc::clone(&list);
        let h2 = tokio::spawn(async move {
            for entry in entries.list_entries {
                apply_aof(&kv2, &l2, &gm2, entry).await;
            }
        });

        let h3 = tokio::spawn(async move {
            for entry in entries.log_entries {
                apply_aof(&kv_store, &list, &group_manager, entry).await;
            }
        });

        let (r1, r2, r3) = tokio::join!(h1, h2, h3);
        r1?;
        r2?;
        r3?;

        println!("[AOF] replay done");
        Ok(())
    }

    pub async fn kv_set(
        &mut self,
        db: String,
        key: String,
        val: Vec<u8>,
        ttl: i64,
    ) -> Result<(), DBError> {
        self.kv_store
            .write()
            .await
            .set(db.clone(), key.clone(), val.clone(), ttl)
            .map_err(DBError::KVSetError)?;
        self.aof.write(AofEntry::KvSet { db, key, val, ttl });
        Ok(())
    }

    pub async fn kv_get(&mut self, db: String, key: String) -> Result<Option<DBEntry>, DBError> {
        self.kv_store
            .write()
            .await
            .get(db, key)
            .map_err(DBError::KVGetError)
    }

    pub async fn kv_del(&mut self, db: String, key: String) -> Result<(), DBError> {
        self.kv_store
            .write()
            .await
            .del(db.clone(), key.clone())
            .map_err(DBError::KVDelError)?;
        self.aof.write(AofEntry::KvDel { db, key });
        Ok(())
    }

    pub async fn kv_keys(&mut self, db: String) -> Result<Vec<String>, DBError> {
        self.kv_store
            .write()
            .await
            .keys(db)
            .map_err(DBError::KVKeysError)
    }

    pub async fn kv_flush(&mut self, db: String) -> Result<(), DBError> {
        self.kv_store
            .write()
            .await
            .flush(db.clone())
            .map_err(DBError::KVFlushError)?;
        self.aof.write(AofEntry::KvFlush { db });
        Ok(())
    }

    pub async fn log_create_group(&mut self, name: &str) -> Result<(), DBError> {
        self.group_manager
            .write()
            .await
            .create_group(name)
            .await
            .map_err(DBError::LogCreateGroupError)?;
        self.aof.write(AofEntry::LogCreateGroup {
            name: name.to_string(),
        });
        Ok(())
    }

    pub async fn log_drop_group(&mut self, name: &str) -> Result<(), DBError> {
        self.group_manager
            .write()
            .await
            .drop_group(name)
            .await
            .map_err(DBError::LogDropGroupError)?;
        self.aof.write(AofEntry::LogDropGroup {
            name: name.to_string(),
        });
        Ok(())
    }

    pub async fn log_add(
        &mut self,
        group: &str,
        timestamp: u64,
        payload: &[u8],
    ) -> Result<u64, DBError> {
        let id = self
            .group_manager
            .write()
            .await
            .add(group, timestamp, payload)
            .await
            .map_err(DBError::LogAppendError)?;
        self.aof.write(AofEntry::LogAdd {
            group: group.to_string(),
            timestamp,
            payload: payload.to_vec(),
        });
        Ok(id)
    }

    pub async fn log_add_range(
        &mut self,
        group: &str,
        entries: &[(u64, &[u8])],
    ) -> Result<(u64, u64), DBError> {
        let result = self
            .group_manager
            .write()
            .await
            .add_range(group, entries)
            .await
            .map_err(DBError::LogAppendRangeError)?;
        self.aof.write(AofEntry::LogAddRange {
            group: group.to_string(),
            entries: entries.iter().map(|(ts, d)| (*ts, d.to_vec())).collect(),
        });
        Ok(result)
    }

    pub async fn log_read(&mut self, group: &str, id: u64) -> Result<(u64, u64, Vec<u8>), DBError> {
        self.group_manager
            .write()
            .await
            .read(group, id)
            .await
            .map_err(DBError::LogReadError)
    }

    pub async fn log_read_range(
        &mut self,
        group: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<(u64, u64, Vec<u8>)>, DBError> {
        self.group_manager
            .write()
            .await
            .read_range(group, start, end)
            .await
            .map_err(DBError::LogReadRangeError)
    }

    pub async fn log_remove(&mut self, group: &str, up_to_id: u64) -> Result<(), DBError> {
        self.group_manager
            .write()
            .await
            .remove(group, up_to_id)
            .await
            .map_err(DBError::LogRemoveError)?;
        self.aof.write(AofEntry::LogRemove {
            group: group.to_string(),
            up_to_id,
        });
        Ok(())
    }

    pub async fn log_list_groups(&mut self) -> Result<Vec<String>, DBError> {
        Ok(self.group_manager.read().await.list_groups())
    }

    pub async fn log_group_stats(&mut self, group: &str) -> Result<GroupStats, DBError> {
        self.group_manager
            .write()
            .await
            .group_stats(group)
            .await
            .map_err(DBError::LogGroupStatsError)
    }

    pub async fn list_push(&mut self, key: String, payload: Vec<u8>) -> Result<(), DBError> {
        self.list
            .write()
            .await
            .push(key.clone(), payload.clone())
            .await
            .map_err(DBError::ListPushError)?;
        self.aof.write(AofEntry::ListPush { key, payload });
        Ok(())
    }

    pub async fn list_push_range(
        &mut self,
        key: String,
        items: Vec<Vec<u8>>,
    ) -> Result<(), DBError> {
        self.list
            .write()
            .await
            .push_range(key.clone(), items.clone())
            .await
            .map_err(DBError::ListPushRangeError)?;
        self.aof.write(AofEntry::ListPushRange { key, items });
        Ok(())
    }

    pub async fn list_pop(&mut self, key: String) -> Result<Vec<u8>, DBError> {
        let data = self
            .list
            .write()
            .await
            .pop(key.clone())
            .await
            .map_err(DBError::ListPopError)?;
        self.aof.write(AofEntry::ListPop { key });
        Ok(data)
    }

    pub async fn list_pop_range(
        &mut self,
        key: String,
        start: usize,
        end: usize,
    ) -> Result<Vec<Vec<u8>>, DBError> {
        let data = self
            .list
            .write()
            .await
            .pop_range(key.clone(), start, end)
            .await
            .map_err(DBError::ListPopRangeError)?;
        self.aof.write(AofEntry::ListPopRange { key, start, end });
        Ok(data)
    }

    pub async fn list_pop_count(
        &mut self,
        key: String,
        count: usize,
    ) -> Result<Vec<Vec<u8>>, DBError> {
        let data = self
            .list
            .write()
            .await
            .pop_count(key.clone(), count)
            .await
            .map_err(DBError::ListPopCountError)?;
        self.aof.write(AofEntry::ListPopCount { key, count });
        Ok(data)
    }

    pub async fn list_len(&mut self, key: String) -> Result<usize, DBError> {
        self.list
            .write()
            .await
            .len(key)
            .await
            .map_err(DBError::ListLenError)
    }

    pub async fn list_flush(&mut self, key: String) -> Result<(), DBError> {
        self.list
            .write()
            .await
            .flush(key.clone())
            .await
            .map_err(DBError::ListFlushError)?;
        self.aof.write(AofEntry::ListFlush { key });
        Ok(())
    }
}

async fn apply_aof(
    kv_store: &Arc<RwLock<KvStore>>,
    list: &Arc<RwLock<SegList>>,
    group_manager: &Arc<RwLock<GroupManager>>,
    entry: AofEntry,
) {
    match entry {
        AofEntry::KvSet { db, key, val, ttl } => {
            let _ = kv_store.write().await.set(db, key, val, ttl);
        }
        AofEntry::KvDel { db, key } => {
            let _ = kv_store.write().await.del(db, key);
        }
        AofEntry::KvFlush { db } => {
            let _ = kv_store.write().await.flush(db);
        }
        AofEntry::LogCreateGroup { name } => {
            let _ = group_manager.write().await.create_group(&name).await;
        }
        AofEntry::LogDropGroup { name } => {
            let _ = group_manager.write().await.drop_group(&name).await;
        }
        AofEntry::LogAdd {
            group,
            timestamp,
            payload,
        } => {
            let _ = group_manager
                .write()
                .await
                .add(&group, timestamp, &payload)
                .await;
        }
        AofEntry::LogAddRange { group, entries } => {
            let owned: Vec<(u64, Vec<u8>)> = entries;
            let refs: Vec<(u64, &[u8])> = owned.iter().map(|(ts, d)| (*ts, d.as_slice())).collect();
            let _ = group_manager.write().await.add_range(&group, &refs).await;
        }
        AofEntry::LogRemove { group, up_to_id } => {
            let _ = group_manager.write().await.remove(&group, up_to_id).await;
        }
        AofEntry::ListPush { key, payload } => {
            let _ = list.write().await.push(key, payload).await;
        }
        AofEntry::ListPushRange { key, items } => {
            let _ = list.write().await.push_range(key, items).await;
        }
        AofEntry::ListPop { key } => {
            let _ = list.write().await.pop(key).await;
        }
        AofEntry::ListPopRange { key, start, end } => {
            let _ = list.write().await.pop_range(key, start, end).await;
        }
        AofEntry::ListPopCount { key, count } => {
            let _ = list.write().await.pop_count(key, count).await;
        }
        AofEntry::ListFlush { key } => {
            let _ = list.write().await.flush(key).await;
        }
    }
}
#[derive(Debug)]
pub enum DBError {
    LogCreateGroupError(GroupError),
    LogDropGroupError(GroupError),
    LogAppendError(GroupError),
    LogAppendRangeError(GroupError),
    LogReadError(GroupError),
    LogReadRangeError(GroupError),
    LogRemoveError(GroupError),
    LogListGroupsError(GroupError),
    LogGroupStatsError(GroupError),
    KVSetError(anyhow::Error),
    KVGetError(anyhow::Error),
    KVDelError(anyhow::Error),
    KVKeysError(anyhow::Error),
    KVFlushError(anyhow::Error),
    ListPushError(ListError),
    ListPushRangeError(ListError),
    ListPopError(ListError),
    ListPopRangeError(ListError),
    ListPopCountError(ListError),
    ListLenError(ListError),
    ListFlushError(ListError),
}

impl std::fmt::Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBError::LogCreateGroupError(e) => write!(f, "failed to create group: {}", e),
            DBError::LogDropGroupError(e) => write!(f, "failed to drop group: {}", e),
            DBError::LogAppendError(e) => write!(f, "failed to append to log: {}", e),
            DBError::LogAppendRangeError(e) => write!(f, "failed to append range to log: {}", e),
            DBError::LogReadError(e) => write!(f, "failed to read from log: {}", e),
            DBError::LogReadRangeError(e) => write!(f, "failed to read range from log: {}", e),
            DBError::LogRemoveError(e) => write!(f, "failed to remove from log: {}", e),
            DBError::LogListGroupsError(e) => write!(f, "failed to list groups: {}", e),
            DBError::LogGroupStatsError(e) => write!(f, "failed to read group stats: {}", e),
            DBError::KVSetError(e) => write!(f, "failed to set key: {}", e),
            DBError::KVGetError(e) => write!(f, "failed to get key: {}", e),
            DBError::KVDelError(e) => write!(f, "failed to del key: {}", e),
            DBError::KVKeysError(e) => write!(f, "failed to return keys: {}", e),
            DBError::KVFlushError(e) => write!(f, "failed to flush: {}", e),
            DBError::ListPushError(e) => write!(f, "failed to push: {}", e),
            DBError::ListPushRangeError(e) => write!(f, "failed to push range: {}", e),
            DBError::ListPopError(e) => write!(f, "failed to pop: {}", e),
            DBError::ListPopRangeError(e) => write!(f, "failed to pop range: {}", e),
            DBError::ListPopCountError(e) => write!(f, "failed to pop count: {}", e),
            DBError::ListLenError(e) => write!(f, "failed to get len: {}", e),
            DBError::ListFlushError(e) => write!(f, "failed to flush list: {}", e),
        }
    }
}
