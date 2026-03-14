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

impl AofEntry {
    fn category(&self) -> EntryCategory {
        match self {
            AofEntry::KvSet { .. } | AofEntry::KvDel { .. } | AofEntry::KvFlush { .. } => {
                EntryCategory::Kv
            }
            AofEntry::LogCreateGroup { .. }
            | AofEntry::LogDropGroup { .. }
            | AofEntry::LogAdd { .. }
            | AofEntry::LogAddRange { .. }
            | AofEntry::LogRemove { .. } => EntryCategory::Log,
            AofEntry::ListPush { .. }
            | AofEntry::ListPushRange { .. }
            | AofEntry::ListPop { .. }
            | AofEntry::ListPopRange { .. }
            | AofEntry::ListPopCount { .. }
            | AofEntry::ListFlush { .. } => EntryCategory::List,
        }
    }
    pub fn to_base64(&self) -> String {
        let json = serde_json::to_vec(self).unwrap();
        const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        let mut i = 0;
        while i + 2 < json.len() {
            let b = ((json[i] as u32) << 16) | ((json[i + 1] as u32) << 8) | (json[i + 2] as u32);
            out.push(CHARS[((b >> 18) & 63) as usize] as char);
            out.push(CHARS[((b >> 12) & 63) as usize] as char);
            out.push(CHARS[((b >> 6) & 63) as usize] as char);
            out.push(CHARS[(b & 63) as usize] as char);
            i += 3;
        }
        match json.len() - i {
            1 => {
                let b = (json[i] as u32) << 16;
                out.push(CHARS[((b >> 18) & 63) as usize] as char);
                out.push(CHARS[((b >> 12) & 63) as usize] as char);
                out.push_str("==");
            }
            2 => {
                let b = ((json[i] as u32) << 16) | ((json[i + 1] as u32) << 8);
                out.push(CHARS[((b >> 18) & 63) as usize] as char);
                out.push(CHARS[((b >> 12) & 63) as usize] as char);
                out.push(CHARS[((b >> 6) & 63) as usize] as char);
                out.push('=');
            }
            _ => {}
        }
        out
    }
}
// TODO: replace with base64 builder
pub fn from_base64(s: &str) -> anyhow::Result<AofEntry> {
    let s = s.as_bytes();
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let decode_char = |c: u8| -> u8 {
        match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => 0,
        }
    };
    let mut i = 0;
    while i + 3 < s.len() {
        let b = ((decode_char(s[i]) as u32) << 18)
            | ((decode_char(s[i + 1]) as u32) << 12)
            | ((decode_char(s[i + 2]) as u32) << 6)
            | (decode_char(s[i + 3]) as u32);
        out.push((b >> 16) as u8);
        if s[i + 2] != b'=' {
            out.push((b >> 8) as u8);
        }
        if s[i + 3] != b'=' {
            out.push(b as u8);
        }
        i += 4;
    }
    Ok(serde_json::from_slice(&out)?)
}

enum EntryCategory {
    Kv,
    Log,
    List,
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

#[derive(Default)]
pub struct AofEntries {
    pub kv_entries: Vec<AofEntry>,
    pub list_entries: Vec<AofEntry>,
    pub log_entries: Vec<AofEntry>,
}

impl AofEntries {
    pub fn len(&self) -> usize {
        self.kv_entries.len() + self.list_entries.len() + self.log_entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub async fn load_aof_separated(path: &Path) -> anyhow::Result<AofEntries> {
    if !path.exists() {
        return Ok(AofEntries::default());
    }

    let mut entries = AofEntries::default();
    let mut lines = BufReader::new(File::open(path).await?).lines();

    while let Some(line) = lines.next_line().await? {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<AofEntry>(&line) {
            Ok(entry) => match entry.category() {
                EntryCategory::Kv => entries.kv_entries.push(entry),
                EntryCategory::Log => entries.log_entries.push(entry),
                EntryCategory::List => entries.list_entries.push(entry),
            },
            Err(e) => eprintln!("[AOF] skipping corrupt line: {}", e),
        }
    }

    Ok(entries)
}

pub async fn load_aof(path: &Path) -> anyhow::Result<Vec<AofEntry>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let mut entries: Vec<AofEntry> = Vec::new();
    let mut lines = BufReader::new(File::open(path).await?).lines();

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
    use super::*;
    use std::{env::temp_dir, time::Duration};
    use tokio::time::sleep;

    async fn write_and_load(filename: &str, entries: Vec<AofEntry>) -> AofEntries {
        let path = temp_dir().join(filename);
        let _ = tokio::fs::remove_file(&path).await;
        let writer = AofWriter::new(path.clone());
        for e in entries {
            writer.write(e);
        }
        drop(writer);
        sleep(Duration::from_millis(100)).await;
        load_aof_separated(&path).await.unwrap()
    }

    #[tokio::test]
    async fn test_load_nonexistent_file() {
        let result = load_aof_separated(Path::new("nonexistent/path/log.aof"))
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_kv_entries_routed_correctly() {
        let loaded = write_and_load(
            "test_kv_routing.aof",
            vec![
                AofEntry::KvSet {
                    db: "d".into(),
                    key: "k".into(),
                    val: vec![1],
                    ttl: 0,
                },
                AofEntry::KvDel {
                    db: "d".into(),
                    key: "k".into(),
                },
                AofEntry::KvFlush { db: "d".into() },
            ],
        )
        .await;

        assert_eq!(loaded.kv_entries.len(), 3);
        assert!(loaded.log_entries.is_empty());
        assert!(loaded.list_entries.is_empty());

        assert!(
            matches!(&loaded.kv_entries[0], AofEntry::KvSet { db, key, val, ttl }
            if db == "d" && key == "k" && val == &[1] && *ttl == 0)
        );
        assert!(matches!(&loaded.kv_entries[1], AofEntry::KvDel { db, key }
            if db == "d" && key == "k"));
        assert!(matches!(&loaded.kv_entries[2], AofEntry::KvFlush { db } if db == "d"));
    }

    #[tokio::test]
    async fn test_log_entries_routed_correctly() {
        let loaded = write_and_load(
            "test_log_routing.aof",
            vec![
                AofEntry::LogCreateGroup { name: "g".into() },
                AofEntry::LogAdd {
                    group: "g".into(),
                    timestamp: 1,
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
                AofEntry::LogDropGroup { name: "g".into() },
            ],
        )
        .await;

        assert_eq!(loaded.log_entries.len(), 5);
        assert!(loaded.kv_entries.is_empty());
        assert!(loaded.list_entries.is_empty());

        assert!(matches!(&loaded.log_entries[0], AofEntry::LogCreateGroup { name } if name == "g"));
        assert!(
            matches!(&loaded.log_entries[3], AofEntry::LogRemove { group, up_to_id }
            if group == "g" && *up_to_id == 5)
        );
    }

    #[tokio::test]
    async fn test_list_entries_routed_correctly() {
        let loaded = write_and_load(
            "test_list_routing.aof",
            vec![
                AofEntry::ListPush {
                    key: "l".into(),
                    payload: vec![1],
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
            ],
        )
        .await;

        assert_eq!(loaded.list_entries.len(), 6);
        assert!(loaded.kv_entries.is_empty());
        assert!(loaded.log_entries.is_empty());

        assert!(matches!(&loaded.list_entries[2], AofEntry::ListPop { key } if key == "l"));
        assert!(
            matches!(&loaded.list_entries[3], AofEntry::ListPopRange { key, start, end }
            if key == "l" && *start == 0 && *end == 3)
        );
    }

    #[tokio::test]
    async fn test_mixed_entries_total_count() {
        let all_entries = vec![
            AofEntry::KvSet {
                db: "d".into(),
                key: "k".into(),
                val: vec![1, 2],
                ttl: 10,
            },
            AofEntry::LogCreateGroup { name: "g1".into() },
            AofEntry::LogAdd {
                group: "g".into(),
                timestamp: 100,
                payload: vec![9],
            },
            AofEntry::ListPush {
                key: "l".into(),
                payload: vec![3, 4],
            },
            AofEntry::ListPop { key: "l".into() },
        ];

        let loaded = write_and_load("test_mixed.aof", all_entries).await;

        assert_eq!(loaded.kv_entries.len(), 1);
        assert_eq!(loaded.log_entries.len(), 2);
        assert_eq!(loaded.list_entries.len(), 2);
        assert_eq!(loaded.len(), 5);
    }

    #[tokio::test]
    async fn test_corrupt_lines_are_skipped() {
        let path = temp_dir().join("test_corrupt.aof");
        tokio::fs::write(
            &path,
            b"{\"op\":\"KvFlush\",\"db\":\"x\"}\nNOT_JSON\n{\"op\":\"KvFlush\",\"db\":\"y\"}\n",
        )
        .await
        .unwrap();

        let entries = load_aof_separated(&path).await.unwrap();
        assert_eq!(entries.kv_entries.len(), 2);
    }

    #[tokio::test]
    async fn test_empty_lines_are_skipped() {
        let path = temp_dir().join("test_empty_lines.aof");
        tokio::fs::write(&path, b"\n\n{\"op\":\"KvFlush\",\"db\":\"x\"}\n\n")
            .await
            .unwrap();

        let entries = load_aof_separated(&path).await.unwrap();
        assert_eq!(entries.kv_entries.len(), 1);
    }

    #[tokio::test]
    async fn test_multiple_writers_append() {
        let path = temp_dir().join("test_multiple_writers.aof");
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

        let entries = load_aof_separated(&path).await.unwrap();
        assert_eq!(entries.kv_entries.len(), 3);
    }
}
