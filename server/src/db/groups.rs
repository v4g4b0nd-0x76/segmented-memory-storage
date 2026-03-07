use futures::TryFutureExt;
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{fs::File, sync::Mutex};

use crate::db::{lru::*, seg_log::*};

#[derive(Debug)]
pub enum GroupError {
    GroupNotFound(String),
    GroupAlreadyExists(String),
    LogError(LogError),
}

impl std::fmt::Display for GroupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GroupError::GroupNotFound(name) => write!(f, "group '{}' not found", name),
            GroupError::GroupAlreadyExists(name) => write!(f, "group '{}' already exists", name),
            GroupError::LogError(e) => write!(f, "log error: {}", e),
        }
    }
}

impl From<LogError> for GroupError {
    fn from(e: LogError) -> Self {
        GroupError::LogError(e)
    }
}

pub struct GroupStats {
    pub total_entries: u64,
    pub total_segments: usize,
    pub next_id: u64,
}

struct Group {
    name: String,
    log: Arc<Mutex<SegLog>>,
    lru_cache: LRU<u64, LogEntry>,
}

impl Group {
    fn new(name: String) -> Self {
        let log = Arc::new(Mutex::new(SegLog::new(name.clone())));
        SegLog::start_periodic_snapshot(Arc::clone(&log), Duration::from_secs(60));
        Group {
            name,
            log,
            lru_cache: LRU::new(100_000, Duration::from_secs(30)),
        }
    }

    fn cache_put(&mut self, id: u64, timestamp: u64, payload: &[u8]) {
        let entry = LogEntry {
            id,
            timestamp,
            len: payload.len(),
            payload: payload.to_vec(),
        };
        self.lru_cache.insert(build_key(&self.name, id), entry);
    }

    fn cache_invalidate(&mut self, id: u64) {
        self.lru_cache.remove(&build_key(&self.name, id));
    }
}

pub struct GroupManager {
    groups: HashMap<String, Group>,
}

const SNAPSHOT_DIR: &str = "snapshots";
const GROUP_NAMES_FILE: &str = "snapshots/group_names";

impl GroupManager {
    pub async fn new() -> Self {
        let mut manager = GroupManager {
            groups: HashMap::new(),
        };
        manager.load_snapshot().await.unwrap_or_else(|e| {
            eprintln!("Failed to load snapshot: {}", e);
        });
        manager
    }

    pub async fn create_group(&mut self, name: &str) -> Result<(), GroupError> {
        if self.groups.contains_key(name) {
            return Err(GroupError::GroupAlreadyExists(name.to_string()));
        }
        let group = Group::new(name.to_string());
        self.groups.insert(name.to_string(), group);
        self.persist_group_name(name).await.unwrap_or_else(|e| {
            eprintln!("Failed to persist group name '{}': {}", name, e);
        });
        Ok(())
    }

    pub async fn drop_group(&mut self, name: &str) -> Result<(), GroupError> {
        if self.groups.remove(name).is_none() {
            return Err(GroupError::GroupNotFound(name.to_string()));
        }
        self.remove_group_from_names(name)
            .await
            .unwrap_or_else(|e| {
                eprintln!("Failed to remove group name '{}': {}", name, e);
            });
        Ok(())
    }

    pub async fn add(
        &mut self,
        group: &str,
        timestamp: u64,
        payload: &[u8],
    ) -> Result<u64, GroupError> {
        let grp = self
            .groups
            .get_mut(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;
        let id = grp.log.lock().await.append(timestamp, payload).await?;
        grp.cache_put(id, timestamp, payload);
        Ok(id)
    }

    pub async fn add_range(
        &mut self,
        group: &str,
        entries: &[(u64, &[u8])],
    ) -> Result<(u64, u64), GroupError> {
        if entries.is_empty() {
            return Err(GroupError::LogError(LogError::EntryNotFound));
        }
        let grp = self
            .groups
            .get_mut(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;

        let first_id = grp
            .log
            .lock()
            .await
            .append(entries[0].0, entries[0].1)
            .await?;
        grp.cache_put(first_id, entries[0].0, entries[0].1);
        let mut last_id = first_id;

        for &(ts, payload) in &entries[1..] {
            let id = grp.log.lock().await.append(ts, payload).await?;
            grp.cache_put(id, ts, payload);
            last_id = id;
        }
        Ok((first_id, last_id))
    }

    pub async fn read(&mut self, group: &str, id: u64) -> Result<(u64, u64, Vec<u8>), GroupError> {
        let grp = self
            .groups
            .get_mut(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;
        let key = build_key(&grp.name, id);
        if let Some(cached) = grp.lru_cache.get(&key) {
            return Ok((cached.id, cached.timestamp, cached.payload.clone()));
        }
        Ok(grp.log.lock().await.read(id)?)
    }

    pub async fn read_range(
        &mut self,
        group: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<(u64, u64, Vec<u8>)>, GroupError> {
        let grp = self
            .groups
            .get_mut(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;

        let range_len = (end - start + 1) as usize;
        let mut cached: Vec<(u64, u64, Vec<u8>)> = Vec::with_capacity(range_len);
        let mut all_cached = true;

        for id in start..=end {
            if let Some(entry) = grp.lru_cache.get(&build_key(&grp.name, id)) {
                cached.push((entry.id, entry.timestamp, entry.payload.clone()));
            } else {
                all_cached = false;
                break;
            }
        }

        if all_cached && cached.len() == range_len {
            return Ok(cached);
        }

        Ok(grp.log.lock().await.read_range(start, end))
    }

    pub async fn remove(&mut self, group: &str, up_to_id: u64) -> Result<(), GroupError> {
        let grp = self
            .groups
            .get_mut(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;
        let removed_ids = grp.log.lock().await.trim(up_to_id);
        for id in removed_ids {
            grp.cache_invalidate(id);
        }
        Ok(())
    }

    pub fn list_groups(&self) -> Vec<String> {
        self.groups.keys().map(|s| s.to_string()).collect()
    }

    pub async fn group_stats(&self, group: &str) -> Result<GroupStats, GroupError> {
        let grp = self
            .groups
            .get(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;
        let log = grp.log.lock().await;
        Ok(GroupStats {
            total_entries: log.total_entries(),
            total_segments: log.total_segments(),
            next_id: log.next_id(),
        })
    }

    async fn persist_group_name(&self, group: &str) -> anyhow::Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        tokio::fs::create_dir_all(SNAPSHOT_DIR).await?;

        let names_path = PathBuf::from(GROUP_NAMES_FILE);
        let mut existing = String::new();

        if tokio::fs::metadata(&names_path).await.is_ok() {
            File::open(&names_path)
                .await?
                .read_to_string(&mut existing)
                .await?;
        }

        let names: Vec<&str> = existing.lines().filter(|s| !s.is_empty()).collect();
        if names.contains(&group) {
            return Ok(());
        }

        let mut all: Vec<String> = names.iter().map(|s| s.to_string()).collect();
        all.push(group.to_string());

        let mut f = File::create(&names_path).await?;
        f.write_all(all.join("\n").as_bytes()).await?;
        Ok(())
    }

    async fn remove_group_from_names(&self, group: &str) -> anyhow::Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let names_path = PathBuf::from(GROUP_NAMES_FILE);
        if tokio::fs::metadata(&names_path).await.is_err() {
            return Ok(());
        }

        let mut contents = String::new();
        File::open(&names_path)
            .await?
            .read_to_string(&mut contents)
            .await?;

        let names: Vec<String> = contents
            .lines()
            .filter(|s| !s.is_empty() && *s != group)
            .map(|s| s.to_string())
            .collect();

        let mut f = File::create(&names_path).await?;
        f.write_all(names.join("\n").as_bytes()).await?;
        Ok(())
    }

    async fn load_snapshot(&mut self) -> anyhow::Result<()> {
        use tokio::io::AsyncReadExt;

        let names_path = PathBuf::from(GROUP_NAMES_FILE);
        if tokio::fs::metadata(&names_path).await.is_err() {
            return Ok(());
        }

        let mut contents = String::new();
        File::open(&names_path)
            .await?
            .read_to_string(&mut contents)
            .await?;

        let group_names: Vec<String> = contents
            .lines()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        for name in group_names {
            let group = Group::new(name.clone());
            group
                .log
                .lock()
                .await
                .load_snapshot()
                .map_err(|e| anyhow::anyhow!("failed to load snapshot for group '{}': {}", name, e))
                .await?;
            self.groups.insert(name, group);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn cleanup() {
        let _ = tokio::fs::remove_file(GROUP_NAMES_FILE).await;
        for name in ["g1", "g2"] {
            let _ = tokio::fs::remove_dir_all(format!("segments/{}", name)).await;
            let _ = tokio::fs::remove_dir_all(format!("snapshots/{}", name)).await;
        }
    }

    #[tokio::test]
    async fn test_full_cycle() {
        cleanup().await;

        let mut m = GroupManager::new().await;

        assert!(m.create_group("g1").await.is_ok());
        assert!(m.create_group("g2").await.is_ok());
        assert!(m.create_group("g1").await.is_err());

        let a1 = m.add("g1", 100, b"aaa").await.unwrap();
        let a2 = m.add("g1", 200, b"bbb").await.unwrap();
        let a3 = m.add("g1", 300, b"ccc").await.unwrap();
        assert_eq!(a1, 1);
        assert_eq!(a2, 2);
        assert_eq!(a3, 3);

        let (rid, rts, rdata) = m.read("g1", a2).await.unwrap();
        assert_eq!(rid, 2);
        assert_eq!(rts, 200);
        assert_eq!(rdata, b"bbb");

        assert!(m.read("g1", 999).await.is_err());
        assert!(m.add("ghost", 0, b"x").await.is_err());
        assert!(m.read("ghost", 1).await.is_err());

        let (first, last) = m
            .add_range("g1", &[(400, b"ddd"), (500, b"eee"), (600, b"fff")])
            .await
            .unwrap();
        assert_eq!(first, 4);
        assert_eq!(last, 6);

        let range = m.read_range("g1", 2, 5).await.unwrap();
        assert_eq!(range.len(), 4);
        assert_eq!(range[0].0, 2);
        assert_eq!(range[3].0, 5);

        assert!(m.read_range("g1", 900, 999).await.unwrap().is_empty());

        let b1 = m.add("g2", 10, b"isolated").await.unwrap();
        assert_eq!(b1, 1);
        let (_, _, d) = m.read("g2", 1).await.unwrap();
        assert_eq!(d, b"isolated");

        m.remove("g1", 4).await.unwrap();
        assert!(m.read("g1", 1).await.is_err());
        assert!(m.read("g1", 2).await.is_err());
        assert!(m.read("g1", 3).await.is_err());
        assert!(m.read("g1", 4).await.is_ok());
        assert!(m.read("g1", 6).await.is_ok());

        let (_, _, kept) = m.read("g2", 1).await.unwrap();
        assert_eq!(kept, b"isolated");

        let stats = m.group_stats("g1").await.unwrap();
        assert_eq!(stats.total_entries, 3);
        assert_eq!(stats.next_id, 7);

        let mut groups = m.list_groups();
        groups.sort();
        assert_eq!(groups, vec!["g1", "g2"]);

        m.drop_group("g2").await.unwrap();
        assert!(m.drop_group("g2").await.is_err());
        assert!(m.read("g2", 1).await.is_err());
        assert_eq!(m.list_groups().len(), 1);

        let post = m.add("g1", 700, b"after-trim").await.unwrap();
        assert_eq!(post, 7);
        let (_, _, pd) = m.read("g1", 7).await.unwrap();
        assert_eq!(pd, b"after-trim");

        cleanup().await;
    }
}
