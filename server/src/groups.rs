use futures::TryFutureExt;
use tokio::fs::File;
// Like redis list you can put entries in groups and each group assign entries for it self
use crate::{
    lru::{LRU, build_key},
    seg_log::{LogEntry, LogError, SegLog},
};
use std::{collections::HashMap, path::PathBuf, time::Duration};
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
    log: SegLog,
    lru_cache: LRU<u64, LogEntry>,
}
impl Group {
    fn add_cache(&mut self, id: u64, timestamp: u64, payload: &[u8]) {
        // TODO: make it group implementation
        let entry = LogEntry {
            id: id,
            timestamp: timestamp,
            len: payload.len(),
            payload: payload.to_vec(),
        };
        let key = build_key(&self.name, id);
        self.lru_cache.insert(key, entry);
    }
}
pub struct GroupManager {
    groups: HashMap<String, Group>,
}

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
        let log = SegLog::new(name.to_string());
        let group = Group {
            name: name.to_string(),
            log,
            lru_cache: LRU::new(100_000, Duration::from_secs(30)),
        };
        self.groups.insert(name.to_string(), group);
        self.snapshot_group(name)
            .await
            .expect(&format!("failed to snapshot group '{}'", name));
        Ok(())
    }
    pub async fn drop_group(&mut self, name: &str) -> Result<(), GroupError> {
        if self.groups.remove(name).is_none() {
            return Err(GroupError::GroupNotFound(name.to_string()));
        }
        self.remove_group_snapshot(name)
            .await
            .expect(&format!("failed to remove snapshot for group '{}'", name));
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
        let id = grp.log.append(timestamp, payload).await?;
        grp.add_cache(id, timestamp, payload);
        Ok(id)
    }

    pub async fn add_range(
        &mut self,
        group: &str,
        entries: &[(u64, &[u8])], // (timestamp, payload)
    ) -> Result<(u64, u64), GroupError> {
        let grp = self
            .groups
            .get_mut(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;

        if entries.is_empty() {
            return Err(GroupError::LogError(LogError::EntryNotFound));
        }

        let first_id = grp.log.append(entries[0].0, entries[0].1).await?;
        grp.add_cache(first_id, entries[0].0, entries[0].1);
        let mut last_id = first_id;
        for &(ts, payload) in &entries[1..] {
            let id = grp.log.append(ts, payload).await?;
            grp.add_cache(id, ts, payload);
            last_id = id;
        }
        Ok((first_id, last_id))
    }
    pub fn read(&mut self, group: &str, id: u64) -> Result<(u64, u64, Vec<u8>), GroupError> {
        let grp = self
            .groups
            .get_mut(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;
        if let Some(cached) = grp.lru_cache.get(&build_key(group, id)) {
            return Ok((cached.id, cached.timestamp, cached.payload.clone()));
        }
        Ok(grp.log.read(id)?)
    }

    pub fn read_range(
        &mut self,
        group: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<(u64, u64, Vec<u8>)>, GroupError> {
        let grp = self
            .groups
            .get_mut(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;

        let range = end - start + 1;
        let mut cached: Vec<(u64, u64, Vec<u8>)> = Vec::with_capacity(range as usize);
        let mut all_cached = true;
        for id in start..=end {
            if let Some(entry) = grp.lru_cache.get(&build_key(group, id)) {
                cached.push((entry.id, entry.timestamp, entry.payload.clone()));
            } else {
                all_cached = false;
                break;
            }
        }
        if all_cached && cached.len() as u64 == range {
            return Ok(cached);
        } else {
            Ok(grp.log.read_range(start, end))
        }
    }

    pub fn remove(&mut self, group: &str, up_to_id: u64) -> Result<(), GroupError> {
        let grp = self
            .groups
            .get_mut(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;
        let to_remove = grp.log.trim(up_to_id);
        for id in &to_remove {
            grp.lru_cache.remove(&build_key(group, *id));
        }
        Ok(())
    }

    pub fn list_groups(&self) -> Vec<&str> {
        self.groups.keys().map(|s| s.as_str()).collect()
    }

    pub fn group_stats(&self, group: &str) -> Result<GroupStats, GroupError> {
        let grp = self
            .groups
            .get(group)
            .ok_or_else(|| GroupError::GroupNotFound(group.to_owned()))?;
        Ok(GroupStats {
            total_entries: grp.log.total_entries(),
            total_segments: grp.log.total_segments(),
            next_id: grp.log.next_id(),
        })
    }
    async fn snapshot_group(&self, group: &str) -> anyhow::Result<()> {
        // check if group_names file not exist create it
        if !tokio::fs::metadata(PathBuf::from("snapshots/group_names"))
            .await
            .is_ok()
        {
            tokio::fs::create_dir_all("snapshots").await?;
            File::create(PathBuf::from("snapshots/group_names")).await?;
        }
        // if group is already listed in group_names file, do nothing
        let mut log_file = File::open(PathBuf::from("snapshots/group_names")).await?;
        let mut contents = String::new();
        use tokio::io::AsyncReadExt;
        log_file.read_to_string(&mut contents).await?;
        let mut group_names: Vec<String> = contents.lines().map(|s| s.to_string()).collect();
        if group_names.contains(&group.to_string()) {
            return Ok(());
        }
        if !group_names.contains(&group.to_string()) {
            group_names.push(group.to_string());
            let new_contents = group_names.join("\n");
            use tokio::io::AsyncWriteExt;
            let mut log_file = File::create(PathBuf::from("snapshots/group_names")).await?;
            log_file.write_all(new_contents.as_bytes()).await?;
        }
        Ok(())
    }
    async fn load_group_names(&self) -> anyhow::Result<Vec<String>> {
        let mut log_file = File::open(PathBuf::from("snapshots/group_names")).await?;
        let mut contents = String::new();
        use tokio::io::AsyncReadExt;
        log_file.read_to_string(&mut contents).await?;
        let group_names: Vec<String> = contents.lines().map(|s| s.to_string()).collect();
        Ok(group_names)
    }
    async fn remove_group_snapshot(&self, group: &str) -> anyhow::Result<()> {
        // if group_names file not exist, do nothing
        if !tokio::fs::metadata(PathBuf::from("snapshots/group_names"))
            .await
            .is_ok()
        {
            return Ok(());
        }
        let mut log_file = File::open(PathBuf::from("snapshots/group_names")).await?;
        let mut contents = String::new();
        use tokio::io::AsyncReadExt;
        log_file.read_to_string(&mut contents).await?;
        let group_names: Vec<String> = contents
            .lines()
            .map(|s| s.to_string())
            .filter(|s| s != group)
            .collect();
        let new_contents = group_names.join("\n");
        use tokio::io::AsyncWriteExt;
        let mut log_file = File::create(PathBuf::from("snapshots/group_names")).await?;
        log_file.write_all(new_contents.as_bytes()).await?;
        Ok(())
    }

    async fn load_snapshot(&mut self) -> anyhow::Result<()> {
        // load snapshot for each group
        let group_names = self.load_group_names().await?;
        for grp in group_names {
            let grp = grp.clone();
            let group = self.groups.get_mut(&grp).ok_or_else(|| {
                anyhow::anyhow!(
                    "group '{}' listed in snapshot but not found in manager",
                    grp
                )
            })?;
            group
                .log
                .load_snapshot()
                .map_err(|e| anyhow::anyhow!("failed to load snapshot for group '{}': {}", grp, e))
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_full_cycle() {
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

        let (rid, rts, rdata) = m.read("g1", a2).unwrap();
        assert_eq!(rid, 2);
        assert_eq!(rts, 200);
        assert_eq!(rdata, b"bbb");

        assert!(m.read("g1", 999).is_err());
        assert!(m.add("ghost", 0, b"x").await.is_err());
        assert!(m.read("ghost", 1).is_err());

        let (first, last) = m
            .add_range("g1", &[(400, b"ddd"), (500, b"eee"), (600, b"fff")])
            .await
            .unwrap();
        assert_eq!(first, 4);
        assert_eq!(last, 6);

        let range = m.read_range("g1", 2, 5).unwrap();
        assert_eq!(range.len(), 4);
        assert_eq!(range[0].0, 2);
        assert_eq!(range[3].0, 5);

        assert!(m.read_range("g1", 900, 999).unwrap().is_empty());

        let b1 = m.add("g2", 10, b"isolated").await.unwrap();
        assert_eq!(b1, 1);
        let (_, _, d) = m.read("g2", 1).unwrap();
        assert_eq!(d, b"isolated");

        m.remove("g1", 4).unwrap();
        assert!(m.read("g1", 1).is_err());
        assert!(m.read("g1", 2).is_err());
        assert!(m.read("g1", 3).is_err());
        assert!(m.read("g1", 4).is_ok());
        assert!(m.read("g1", 6).is_ok());

        let (_, _, kept) = m.read("g2", 1).unwrap();
        assert_eq!(kept, b"isolated");

        let stats = m.group_stats("g1").unwrap();
        assert_eq!(stats.total_entries, 3);
        assert_eq!(stats.next_id, 7);

        let mut groups = m.list_groups();
        groups.sort();
        assert_eq!(groups, vec!["g1", "g2"]);

        m.drop_group("g2").await.unwrap();
        assert!(m.drop_group("g2").await.is_err());
        assert!(m.read("g2", 1).is_err());
        assert_eq!(m.list_groups().len(), 1);

        let post = m.add("g1", 700, b"after-trim").await.unwrap();
        assert_eq!(post, 7);
        let (_, _, pd) = m.read("g1", 7).unwrap();
        assert_eq!(pd, b"after-trim");
    }
}
