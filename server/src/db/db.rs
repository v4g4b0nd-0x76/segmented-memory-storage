use anyhow::anyhow;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::db::{
    groups::{GroupError, GroupManager, GroupStats},
    kv::{DBEntry, KvStore},
    seg_list::{ListError, SegList},
};

pub struct DB {
    group_manager: Arc<RwLock<GroupManager>>,
    kv_store: Arc<RwLock<KvStore>>,
    list: Arc<RwLock<SegList>>,
}

impl DB {
    pub async fn new() -> Self {
        DB {
            group_manager: Arc::new(RwLock::new(GroupManager::new().await)),
            kv_store: Arc::new(RwLock::new(KvStore::new(None).await)),
            list: Arc::new(RwLock::new(SegList::new().await)),
        }
    }
    pub async fn kv_set(
        &mut self,
        db: String,
        key: String,
        val: Vec<u8>,
        ttl: i64,
    ) -> Result<(), DBError> {
        match self.kv_store.write().await.set(db, key, val, ttl).await {
            Ok(_) => Ok(()),
            Err(err) => Err(DBError::KVSetError(err)),
        }
    }
    pub async fn kv_get(&mut self, db: String, key: String) -> Result<Option<DBEntry>, DBError> {
        match self.kv_store.write().await.get(db, key).await {
            Ok(entry) => Ok(entry),
            Err(err) => Err(DBError::KVSetError(err)),
        }
    }
    pub async fn kv_del(&mut self, db: String, key: String) -> Result<(), DBError> {
        match self.kv_store.write().await.del(db, key).await {
            Ok(_) => Ok(()),
            Err(err) => Err(DBError::KVSetError(err)),
        }
    }
    pub async fn kv_keys(&mut self, db: String) -> Result<Vec<String>, DBError> {
        match self.kv_store.write().await.keys(db).await {
            Ok(keys) => Ok(keys),
            Err(err) => Err(DBError::KVSetError(err)),
        }
    }
    pub async fn kv_flush(&mut self, db: String) -> Result<(), DBError> {
        match self.kv_store.write().await.flush(db).await {
            Ok(_) => Ok(()),
            Err(err) => Err(DBError::KVSetError(err)),
        }
    }

    pub async fn log_create_group(&mut self, name: &str) -> Result<(), DBError> {
        match self.group_manager.write().await.create_group(name).await {
            Ok(_) => Ok(()),
            Err(err) => Err(DBError::LogCreateGroupError(err)),
        }
    }
    pub async fn log_drop_group(&mut self, name: &str) -> Result<(), DBError> {
        match self.group_manager.write().await.drop_group(name).await {
            Ok(_) => Ok(()),
            Err(err) => Err(DBError::LogDropGroupError(err)),
        }
    }
    pub async fn log_add(
        &mut self,
        group: &str,
        timestamp: u64,
        payload: &[u8],
    ) -> Result<u64, DBError> {
        match self
            .group_manager
            .write()
            .await
            .add(group, timestamp, payload)
            .await
        {
            Ok(id) => Ok(id),
            Err(err) => Err(DBError::LogAppendError(err)),
        }
    }
    pub async fn log_add_range(
        &mut self,
        group: &str,
        entries: &[(u64, &[u8])],
    ) -> Result<(u64, u64), DBError> {
        match self
            .group_manager
            .write()
            .await
            .add_range(group, entries)
            .await
        {
            Ok((first, last)) => Ok((first, last)),
            Err(err) => Err(DBError::LogAppendRangeError(err)),
        }
    }
    pub async fn log_read(&mut self, group: &str, id: u64) -> Result<(u64, u64, Vec<u8>), DBError> {
        match self.group_manager.write().await.read(group, id).await {
            Ok((id, ts, data)) => Ok((id, ts, data)),
            Err(err) => Err(DBError::LogReadError(err)),
        }
    }
    pub async fn log_read_range(
        &mut self,
        group: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<(u64, u64, Vec<u8>)>, DBError> {
        match self
            .group_manager
            .write()
            .await
            .read_range(group, start, end)
            .await
        {
            Ok((data)) => Ok(data),
            Err(err) => Err(DBError::LogReadRangeError(err)),
        }
    }
    pub async fn log_remove(&mut self, group: &str, up_to_id: u64) -> Result<(), DBError> {
        match self
            .group_manager
            .write()
            .await
            .remove(group, up_to_id)
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(DBError::LogRemoveError(err)),
        }
    }
    pub async fn log_list_groups(&mut self) -> Result<Vec<String>, DBError> {
        Ok(self.group_manager.read().await.list_groups())
    }

    pub async fn log_group_stats(&mut self, group: &str) -> Result<GroupStats, DBError> {
        match self.group_manager.write().await.group_stats(group).await {
            Ok(stats) => Ok(stats),
            Err(err) => Err(DBError::LogGroupStatsError(err)),
        }
    }

    pub async fn list_push(&mut self, key: String, payload: Vec<u8>) -> Result<(), DBError> {
        match self.list.write().await.push(key, payload).await {
            Ok(_) => Ok(()),
            Err(err) => Err(DBError::ListPushError(err)),
        }
    }
    pub async fn list_push_range(
        &mut self,
        key: String,
        items: Vec<Vec<u8>>,
    ) -> Result<(), DBError> {
        match self.list.write().await.push_range(key, items).await {
            Ok(_) => todo!(),
            Err(err) => Err(DBError::ListPushRangeError(err)),
        }
    }
    pub async fn list_pop(&mut self, key: String) -> Result<Vec<u8>, DBError> {
        match self.list.write().await.pop(key).await {
            Ok(data) => Ok(data),
            Err(err) => Err(DBError::ListPopError(err)),
        }
    }
    pub async fn list_pop_range(
        &mut self,
        key: String,
        start: usize,
        end: usize,
    ) -> Result<Vec<Vec<u8>>, DBError> {
        match self.list.write().await.pop_range(key, start, end).await {
            Ok(data) => Ok(data),
            Err(err) => Err(DBError::ListPopRangeError(err)),
        }
    }
    pub async fn list_pop_count(
        &mut self,
        key: String,
        count: usize,
    ) -> Result<Vec<Vec<u8>>, DBError> {
        match self.list.write().await.pop_count(key, count).await {
            Ok(data) => Ok(data),
            Err(err) => Err(DBError::ListPopCountError(err)),
        }
    }
    pub async fn list_len(&mut self, key: String) -> Result<usize, DBError> {
        match self.list.write().await.len(key).await {
            Ok(len) => Ok(len),
            Err(err) => Err(DBError::ListLenError(err)),
        }
    }
    pub async fn list_flush(&mut self, key: String) -> Result<(), DBError> {
        match self.list.write().await.flush(key).await {
            Ok(len) => Ok(len),
            Err(err) => Err(DBError::ListFlushError(err)),
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

    // list_push
    // list_push_range
    // list_pop
    // list_pop_range
    // list_pop_count
    // list_len
    // list_flush
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
            DBError::LogCreateGroupError(err) => {
                write!(f, "failed to create group: {}", err.to_string())
            }
            DBError::LogDropGroupError(err) => {
                write!(f, "failed to drop group: {}", err.to_string())
            }
            DBError::LogAppendError(err) => {
                write!(f, "failed to append to log: {}", err.to_string())
            }
            DBError::LogAppendRangeError(err) => {
                write!(
                    f,
                    "failed to append range of entities to log: {}",
                    err.to_string()
                )
            }
            DBError::LogReadError(err) => {
                write!(f, "failed to read id from log: {}", err.to_string())
            }
            DBError::LogReadRangeError(err) => {
                write!(f, "failed to read id from log: {}", err.to_string())
            }
            DBError::LogRemoveError(err) => {
                write!(f, "failed to remove id from log: {}", err.to_string())
            }
            DBError::LogListGroupsError(err) => {
                write!(f, "failed to list groups: {}", err.to_string())
            }
            DBError::LogGroupStatsError(err) => {
                write!(f, "failed to read group stats: {}", err.to_string())
            }
            DBError::KVSetError(err) => writeln!(f, "failed to set key: {}", err.to_string()),
            DBError::KVGetError(err) => writeln!(f, "failed to get key: {}", err.to_string()),
            DBError::KVDelError(err) => writeln!(f, "failed to del key: {}", err.to_string()),
            DBError::KVKeysError(err) => writeln!(f, "failed to return keys: {}", err.to_string()),
            DBError::KVFlushError(err) => writeln!(f, "failed to flush keys: {}", err.to_string()),
            DBError::ListPushError(err) => write!(f, "failed to push: {}", err.to_string()),
            DBError::ListPushRangeError(err) => {
                write!(f, "failed to push range: {}", err.to_string())
            }
            DBError::ListPopError(err) => write!(f, "failed to pop: {}", err.to_string()),
            DBError::ListPopRangeError(err) => {
                write!(f, "failed to pop range: {}", err.to_string())
            }
            DBError::ListPopCountError(err) => {
                write!(f, "failed to pop count: {}", err.to_string())
            }
            DBError::ListLenError(err) => write!(f, "failed to count len: {}", err.to_string()),
            DBError::ListFlushError(err) => write!(f, "failed to flush list: {}", err.to_string()),
        }
    }
}

struct LogReadResult {
    pub id: u64,
    pub ts: u64,
    pub data: Vec<u8>,
}
