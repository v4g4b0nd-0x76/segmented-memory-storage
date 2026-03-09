use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, RwLock, mpsc};

use crate::db::db::{DB, DBError};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Resource {
    Kv,
    List,
    Log,
}

#[derive(Debug, Clone)]
pub enum PipelineCommand {
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
    ListPush {
        key: String,
        payload: Vec<u8>,
    },
    ListPop {
        key: String,
    },
    ListFlush {
        key: String,
    },
    LogAdd {
        group: String,
        timestamp: u64,
        payload: Vec<u8>,
    },
}

impl PipelineCommand {
    fn resource(&self) -> Resource {
        match self {
            PipelineCommand::KvSet { .. }
            | PipelineCommand::KvDel { .. }
            | PipelineCommand::KvFlush { .. } => Resource::Kv,
            PipelineCommand::ListPush { .. }
            | PipelineCommand::ListPop { .. }
            | PipelineCommand::ListFlush { .. } => Resource::List,
            PipelineCommand::LogAdd { .. } => Resource::Log,
        }
    }
}

#[derive(Debug)]
pub enum PipelineError {
    SendError,
    ExecError(DBError),
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineError::SendError => write!(f, "pipeline channel closed"),
            PipelineError::ExecError(e) => write!(f, "pipeline exec error: {}", e),
        }
    }
}

#[derive(Debug)]
pub struct Pipeline {
    pub id: u64,
    commands: Vec<PipelineCommand>,
    tx: mpsc::Sender<PipelineJob>,
}

impl Pipeline {
    fn new(id: u64, tx: mpsc::Sender<PipelineJob>) -> Self {
        Pipeline {
            id,
            commands: Vec::new(),
            tx,
        }
    }

    pub fn kv_set(&mut self, db: String, key: String, val: Vec<u8>, ttl: i64) -> &mut Self {
        self.commands
            .push(PipelineCommand::KvSet { db, key, val, ttl });
        self
    }

    pub fn kv_del(&mut self, db: String, key: String) -> &mut Self {
        self.commands.push(PipelineCommand::KvDel { db, key });
        self
    }

    pub fn kv_flush(&mut self, db: String) -> &mut Self {
        self.commands.push(PipelineCommand::KvFlush { db });
        self
    }

    pub fn list_push(&mut self, key: String, payload: Vec<u8>) -> &mut Self {
        self.commands
            .push(PipelineCommand::ListPush { key, payload });
        self
    }

    pub fn list_pop(&mut self, key: String) -> &mut Self {
        self.commands.push(PipelineCommand::ListPop { key });
        self
    }

    pub fn list_flush(&mut self, key: String) -> &mut Self {
        self.commands.push(PipelineCommand::ListFlush { key });
        self
    }

    pub fn log_add(&mut self, group: String, timestamp: u64, payload: Vec<u8>) -> &mut Self {
        self.commands.push(PipelineCommand::LogAdd {
            group,
            timestamp,
            payload,
        });
        self
    }

    pub async fn end(self) -> Result<(), PipelineError> {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let job = PipelineJob {
            commands: self.commands,
            done: done_tx,
        };
        self.tx
            .send(job)
            .await
            .map_err(|_| PipelineError::SendError)?;
        done_rx.await.map_err(|_| PipelineError::SendError)?
    }
}

struct PipelineJob {
    commands: Vec<PipelineCommand>,
    done: tokio::sync::oneshot::Sender<Result<(), PipelineError>>,
}

#[derive(Default)]
struct LockState {
    kv_holders: u32,
    list_holders: u32,
    log_holders: u32,
}

impl LockState {
    fn can_acquire(&self, resources: &[Resource]) -> bool {
        for r in resources {
            match r {
                Resource::Kv => {
                    if self.kv_holders > 0 && resources.contains(&Resource::List)
                        || self.kv_holders > 0 && resources.contains(&Resource::Log)
                    {
                        return false;
                    }
                }
                _ => {}
            }
        }
        for r in resources {
            match r {
                Resource::Kv => {
                    if self.kv_holders > 0 {
                        return false;
                    }
                }
                Resource::List => {
                    if self.list_holders > 0 {
                        return false;
                    }
                }
                Resource::Log => {
                    if self.log_holders > 0 {
                        return false;
                    }
                }
            }
        }
        true
    }

    fn acquire(&mut self, resources: &[Resource]) {
        for r in resources {
            match r {
                Resource::Kv => self.kv_holders += 1,
                Resource::List => self.list_holders += 1,
                Resource::Log => self.log_holders += 1,
            }
        }
    }

    fn release(&mut self, resources: &[Resource]) {
        for r in resources {
            match r {
                Resource::Kv => self.kv_holders = self.kv_holders.saturating_sub(1),
                Resource::List => self.list_holders = self.list_holders.saturating_sub(1),
                Resource::Log => self.log_holders = self.log_holders.saturating_sub(1),
            }
        }
    }
}

fn unique_resources(commands: &[PipelineCommand]) -> Vec<Resource> {
    let mut seen = std::collections::HashSet::new();
    commands
        .iter()
        .map(|c| c.resource())
        .filter(|r| seen.insert(r.clone()))
        .collect()
}

struct QueuedJob {
    op_count: usize,
    resources: Vec<Resource>,
    job: PipelineJob,
}

pub struct LockManager {
    state: Arc<RwLock<LockState>>,
    notify: Arc<Notify>,
    queue: Arc<RwLock<Vec<QueuedJob>>>,
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            state: Arc::new(RwLock::new(LockState::default())),
            notify: Arc::new(Notify::new()),
            queue: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn enqueue(&self, job: PipelineJob) {
        let resources = unique_resources(&job.commands);
        let op_count = job.commands.len();
        let queued = QueuedJob {
            op_count,
            resources,
            job,
        };

        {
            let mut q = self.queue.write().await;
            let pos = q.partition_point(|j| j.op_count <= queued.op_count);
            q.insert(pos, queued);
        }

        self.notify.notify_one();
    }

    pub async fn acquire(&self, resources: &[Resource]) {
        loop {
            {
                let mut state = self.state.write().await;
                if state.can_acquire(resources) {
                    state.acquire(resources);
                    return;
                }
            }
            self.notify.notified().await;
        }
    }

    pub async fn release(&self, resources: &[Resource]) {
        {
            let mut state = self.state.write().await;
            state.release(resources);
        }
        self.notify.notify_waiters();
    }

    pub async fn try_pop_runnable(&self) -> Option<(PipelineJob, Vec<Resource>)> {
        let state = self.state.read().await;
        let mut q = self.queue.write().await;

        let pos = q.iter().position(|j| state.can_acquire(&j.resources))?;
        let queued = q.remove(pos);
        Some((queued.job, queued.resources))
    }
}

pub struct PipelineManager {
    tx: mpsc::Sender<PipelineJob>,
    pipelines: BTreeMap<u64, Arc<Mutex<Pipeline>>>,
    next_id: u64,
}

impl PipelineManager {
    pub fn new(db: Arc<RwLock<DB>>) -> Self {
        let (tx, mut rx) = mpsc::channel::<PipelineJob>(256);
        let lock_manager = Arc::new(LockManager::new());

        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                let lm = Arc::clone(&lock_manager);
                let db = Arc::clone(&db);

                lm.enqueue(job).await;

                loop {
                    while let Ok(extra) = rx.try_recv() {
                        lm.enqueue(extra).await;
                    }

                    match lm.try_pop_runnable().await {
                        None => break,
                        Some((runnable_job, resources)) => {
                            let lm2 = Arc::clone(&lm);
                            let db2 = Arc::clone(&db);

                            tokio::spawn(async move {
                                lm2.acquire(&resources).await;
                                let result = execute_job(&db2, runnable_job.commands).await;
                                lm2.release(&resources).await;
                                let _ = runnable_job.done.send(result);
                            });
                        }
                    }
                }
            }
        });

        PipelineManager {
            tx,
            pipelines: BTreeMap::new(),
            next_id: 0,
        }
    }

    pub fn start(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        let pipeline = Arc::new(Mutex::new(Pipeline::new(id, self.tx.clone())));
        self.pipelines.insert(id, pipeline);
        id
    }

    pub fn get(&self, id: u64) -> Option<Arc<Mutex<Pipeline>>> {
        self.pipelines.get(&id).cloned()
    }

    pub fn take(&mut self, id: u64) -> Option<Pipeline> {
        let arc = self.pipelines.remove(&id)?;
        Arc::try_unwrap(arc).ok().map(|m| m.into_inner().unwrap())
    }

    pub fn remove(&mut self, id: u64) {
        self.pipelines.remove(&id);
    }
}

async fn execute_job(
    db: &Arc<RwLock<DB>>,
    commands: Vec<PipelineCommand>,
) -> Result<(), PipelineError> {
    let mut db = db.write().await;
    for cmd in commands {
        let result = match cmd {
            PipelineCommand::KvSet {
                db: d,
                key,
                val,
                ttl,
            } => db.kv_set(d, key, val, ttl).await.map(|_| ()),
            PipelineCommand::KvDel { db: d, key } => db.kv_del(d, key).await.map(|_| ()),
            PipelineCommand::KvFlush { db: d } => db.kv_flush(d).await.map(|_| ()),
            PipelineCommand::ListPush { key, payload } => {
                db.list_push(key, payload).await.map(|_| ())
            }
            PipelineCommand::ListPop { key } => db.list_pop(key).await.map(|_| ()),
            PipelineCommand::ListFlush { key } => db.list_flush(key).await.map(|_| ()),
            PipelineCommand::LogAdd {
                group,
                timestamp,
                payload,
            } => db
                .log_add(group.as_str(), timestamp, payload.as_slice())
                .await
                .map(|_| ()),
        };
        if let Err(e) = result {
            eprintln!("[pipeline] command failed: {}", e);
            return Err(PipelineError::ExecError(e));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_manager() -> (PipelineManager, Arc<RwLock<DB>>) {
        let db = Arc::new(RwLock::new(DB::new().await));
        let manager = PipelineManager::new(Arc::clone(&db));
        (manager, db)
    }

    #[tokio::test]
    async fn test_single_kv_set() {
        let (mut manager, db) = make_manager().await;
        let id = manager.start();
        manager.get(id).unwrap().lock().unwrap().kv_set(
            "default".into(),
            "k1".into(),
            b"v1".to_vec(),
            0,
        );
        manager.take(id).unwrap().end().await.unwrap();

        let val = db
            .write()
            .await
            .kv_get("default".into(), "k1".into())
            .await
            .unwrap();
        assert!(val.is_some());
    }

    #[tokio::test]
    async fn test_concurrent_kv_and_list() {
        let (mut manager, db) = make_manager().await;
        let manager = Arc::new(Mutex::new(manager));

        let m1 = Arc::clone(&manager);
        let m2 = Arc::clone(&manager);

        let h1 = tokio::spawn(async move {
            let id = m1.lock().unwrap().start();
            m1.lock().unwrap().get(id).unwrap().lock().unwrap().kv_set(
                "default".into(),
                "ck".into(),
                b"v".to_vec(),
                0,
            );
            let pipeline = m1.lock().unwrap().take(id).unwrap();
            pipeline.end().await.unwrap();
        });

        let h2 = tokio::spawn(async move {
            let id = m2.lock().unwrap().start();
            m2.lock()
                .unwrap()
                .get(id)
                .unwrap()
                .lock()
                .unwrap()
                .list_push("cl".into(), b"item".to_vec());
            let pipeline = m2.lock().unwrap().take(id).unwrap();
            pipeline.end().await.unwrap();
        });

        h1.await.unwrap();
        h2.await.unwrap();

        let mut db = db.write().await;
        assert!(
            db.kv_get("default".into(), "ck".into())
                .await
                .unwrap()
                .is_some()
        );
        assert!(db.list_pop("cl".into()).await.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_ordering_by_op_count() {
        let (mut manager, _db) = make_manager().await;
        let manager = Arc::new(Mutex::new(manager));

        let mut handles = vec![];
        for i in 0..5u32 {
            let m = Arc::clone(&manager);
            let ops = (5 - i) as usize;
            handles.push(tokio::spawn(async move {
                let id = m.lock().unwrap().start();
                {
                    let mut mgr = m.lock().unwrap();
                    let arc = mgr.get(id).unwrap();
                    let mut p = arc.lock().unwrap();
                    for j in 0..ops {
                        p.kv_set(
                            "default".into(),
                            format!("ord_{}_{}", i, j),
                            b"v".to_vec(),
                            0,
                        );
                    }
                }
                let pipeline = m.lock().unwrap().take(id).unwrap();
                pipeline.end().await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_empty_pipeline() {
        let (mut manager, _db) = make_manager().await;
        let id = manager.start();
        assert!(manager.take(id).unwrap().end().await.is_ok());
    }

    #[tokio::test]
    async fn test_kv_flush() {
        let (mut manager, db) = make_manager().await;
        db.write()
            .await
            .kv_set("default".into(), "fk".into(), b"v".to_vec(), 0)
            .await
            .unwrap();

        let id = manager.start();
        manager
            .get(id)
            .unwrap()
            .lock()
            .unwrap()
            .kv_flush("default".into());
        manager.take(id).unwrap().end().await.unwrap();

        let val = db
            .write()
            .await
            .kv_get("default".into(), "fk".into())
            .await
            .unwrap();
        assert!(val.is_none());
    }

    #[tokio::test]
    async fn test_get_and_remove_pipeline() {
        let (mut manager, _db) = make_manager().await;
        let id = manager.start();

        assert!(manager.get(id).is_some());
        manager.remove(id);
        assert!(manager.get(id).is_none());
    }
}
