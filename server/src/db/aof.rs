use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::mpsc,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "op")]
pub enum AofEntry {
    KvSet {
        db: String,
        key: String,
        val: Vec<u8>,
        ttl: i64,
    },
    KvDel {
        db: String,
        key: String,
    },
    KvFlush {
        db: String,
    },
    LogCreateGroup {
        name: String,
    },
    LogDropGroup {
        name: String,
    },
    LogAdd {
        group: String,
        timestamp: u64,
        payload: Vec<u8>,
    },
    LogAddRange {
        group: String,
        entries: Vec<(u64, Vec<u8>)>,
    },
    LogRemove {
        group: String,
        up_to_id: u64,
    },
    ListPush {
        key: String,
        payload: Vec<u8>,
    },
    ListPushRange {
        key: String,
        items: Vec<Vec<u8>>,
    },
    ListPop {
        key: String,
    },
    ListPopRange {
        key: String,
        start: usize,
        end: usize,
    },
    ListPopCount {
        key: String,
        count: usize,
    },
    ListFlush {
        key: String,
    },
}

pub struct AofWriter {
    tx: mpsc::UnboundedSender<AofEntry>,
}

impl AofWriter {
    pub fn new(path: PathBuf) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(aof_worker(path, rx));
        AofWriter { tx }
    }

    pub fn write(&self, entry: AofEntry) {
        let _ = self.tx.send(entry);
    }
}

async fn aof_worker(path: PathBuf, mut rx: mpsc::UnboundedReceiver<AofEntry>) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent).await;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
        .expect("failed to open AOF file");

    while let Some(entry) = rx.recv().await {
        if let Ok(mut line) = serde_json::to_string(&entry) {
            line.push('\n');
            let _ = file.write_all(line.as_bytes()).await;
        }
    }
}

pub async fn load_aof(path: &Path) -> anyhow::Result<Vec<AofEntry>> {
    if !path.exists() {
        return Ok(vec![]);
    }

    let file = File::open(path).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut entries = Vec::new();

    while let Some(line) = lines.next_line().await? {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<AofEntry>(&line) {
            Ok(entry) => entries.push(entry),
            Err(e) => eprintln!("[AOF] skipping corrupt line: {}", e),
        }
    }

    Ok(entries)
}
#[cfg(test)]
mod tests {
    use std::{env::temp_dir, time::Duration};

    use super::*;

    use tokio::time::sleep;

    #[tokio::test]
    async fn test_load_nonexistent_file() {
        let result = load_aof(Path::new("nonexistent/path/log.aof"))
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_write_and_load_single_entry() {
        let dir = temp_dir();
        let path = dir.join("test_write_and_load_single_entry.aof");
        let _ = tokio::fs::remove_file(&path).await;

        let writer = AofWriter::new(path.clone());
        writer.write(AofEntry::KvSet {
            db: "mydb".into(),
            key: "foo".into(),
            val: b"bar".to_vec(),
            ttl: 0,
        });

        drop(writer);
        sleep(Duration::from_millis(100)).await;

        let entries = load_aof(&path).await.unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            AofEntry::KvSet { db, key, val, ttl } => {
                assert_eq!(db, "mydb");
                assert_eq!(key, "foo");
                assert_eq!(val, b"bar");
                assert_eq!(*ttl, 0);
            }
            _ => panic!("unexpected entry type"),
        }
    }

    #[tokio::test]
    async fn test_write_and_load_all_variants() {
        let dir = temp_dir();
        let path = dir.join("test_write_and_load_all_variants.aof");
        let _ = tokio::fs::remove_file(&path).await;

        let writer = AofWriter::new(path.clone());

        let entries = vec![
            AofEntry::KvSet {
                db: "d".into(),
                key: "k".into(),
                val: vec![1, 2],
                ttl: 10,
            },
            AofEntry::KvDel {
                db: "d".into(),
                key: "k".into(),
            },
            AofEntry::KvFlush { db: "d".into() },
            AofEntry::LogCreateGroup { name: "g1".into() },
            AofEntry::LogDropGroup { name: "g1".into() },
            AofEntry::LogAdd {
                group: "g".into(),
                timestamp: 100,
                payload: vec![9],
            },
            AofEntry::LogAddRange {
                group: "g".into(),
                entries: vec![(1, vec![1]), (2, vec![2])],
            },
            AofEntry::LogRemove {
                group: "g".into(),
                up_to_id: 5,
            },
            AofEntry::ListPush {
                key: "l".into(),
                payload: vec![3, 4],
            },
            AofEntry::ListPushRange {
                key: "l".into(),
                items: vec![vec![1], vec![2]],
            },
            AofEntry::ListPop { key: "l".into() },
            AofEntry::ListPopRange {
                key: "l".into(),
                start: 0,
                end: 3,
            },
            AofEntry::ListPopCount {
                key: "l".into(),
                count: 2,
            },
            AofEntry::ListFlush { key: "l".into() },
        ];

        for e in &entries {
            writer.write(e.clone());
        }

        drop(writer);
        sleep(Duration::from_millis(100)).await;

        let loaded = load_aof(&path).await.unwrap();
        assert_eq!(loaded.len(), entries.len());
    }

    #[tokio::test]
    async fn test_corrupt_lines_are_skipped() {
        let dir = temp_dir();
        let path = dir.join("test_corrupt_lines_are_skipped.aof");

        tokio::fs::write(
            &path,
            b"{\"op\":\"KvFlush\",\"db\":\"x\"}\nNOT_JSON\n{\"op\":\"KvFlush\",\"db\":\"y\"}\n"
                as &[u8],
        )
        .await
        .unwrap();

        let entries = load_aof(&path).await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_multiple_writers_append() {
        let dir = temp_dir();
        let path = dir.join("test_multiple_writers_append.aof");
        let _ = tokio::fs::remove_file(&path).await;

        for i in 0..3u32 {
            let writer = AofWriter::new(path.clone());
            writer.write(AofEntry::KvSet {
                db: "d".into(),
                key: format!("k{}", i),
                val: vec![i as u8],
                ttl: 0,
            });
            drop(writer);
            sleep(Duration::from_millis(50)).await;
        }

        let entries = load_aof(&path).await.unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[tokio::test]
    async fn test_empty_lines_are_skipped() {
        let dir = temp_dir();
        let path = dir.join("test_empty_lines_are_skipped.aof");

        tokio::fs::write(&path, b"\n\n{\"op\":\"KvFlush\",\"db\":\"x\"}\n\n")
            .await
            .unwrap();

        let entries = load_aof(&path).await.unwrap();
        assert_eq!(entries.len(), 1);
    }
}
