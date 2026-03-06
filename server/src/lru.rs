use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};
#[derive(Clone, Debug)]
struct Entry<V> {
    value: V,
    expires_at: Instant,
    prev: usize,
    next: usize,
}

#[derive(Clone, Debug)]
pub struct LRU<K, V> {
    map: HashMap<K, usize>,
    reverse: Vec<Option<K>>,
    entries: Vec<Option<Entry<V>>>,
    free: Vec<usize>,
    head: usize,
    tail: usize,
    ttl: Duration,
    cap: usize,
}

const NULL: usize = usize::MAX;

impl<K, V> LRU<K, V>
where
    K: Eq + std::hash::Hash + Clone,
{
    pub fn new(cap: usize, ttl: Duration) -> Self {
        Self {
            map: HashMap::with_capacity(cap),
            reverse: Vec::with_capacity(cap),
            entries: Vec::with_capacity(cap),
            free: Vec::new(),
            head: NULL,
            tail: NULL,
            ttl,
            cap,
        }
    }

    fn alloc_slot(&mut self, key: K, entry: Entry<V>) -> usize {
        if let Some(idx) = self.free.pop() {
            self.entries[idx] = Some(entry);
            self.reverse[idx] = Some(key);
            idx
        } else {
            self.entries.push(Some(entry));
            self.reverse.push(Some(key));
            self.entries.len() - 1
        }
    }

    fn detach(&mut self, idx: usize) {
        let (prev, next) = {
            let e = self.entries[idx].as_ref().unwrap();
            (e.prev, e.next)
        };
        if prev != NULL {
            self.entries[prev].as_mut().unwrap().next = next
        } else {
            self.head = next;
        }
        if next != NULL {
            self.entries[next].as_mut().unwrap().prev = prev;
        } else {
            self.tail = prev;
        }
    }

    fn attach_front(&mut self, idx: usize) {
        let e = self.entries[idx].as_mut().unwrap();
        e.prev = NULL;
        e.next = self.head;
        let _ = e;
        if self.head != NULL {
            self.entries[self.head].as_mut().unwrap().prev = idx;
        } else {
            self.tail = idx;
        }
        self.head = idx;
    }

    pub fn insert(&mut self, key: K, value: V) {
        let expires_at = Instant::now() + self.ttl;

        if let Some(&idx) = self.map.get(&key) {
            self.detach(idx);
            let e = self.entries[idx].as_mut().unwrap();
            e.value = value;
            e.expires_at = expires_at;
            e.prev = NULL;
            e.next = NULL;
            self.attach_front(idx);
            return;
        }

        if self.map.len() == self.cap {
            let evict = self.tail;
            if evict != NULL {
                self.detach(evict);
                if let Some(Some(old_key)) = self.reverse.get_mut(evict) {
                    let old_key = old_key.clone();
                    self.map.remove(&old_key);
                }
                self.entries[evict] = None;
                self.reverse[evict] = None;
                self.free.push(evict);
            }
        }

        let idx = self.alloc_slot(
            key.clone(),
            Entry {
                value,
                expires_at,
                prev: NULL,
                next: NULL,
            },
        );
        self.map.insert(key, idx);
        self.attach_front(idx);
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        let idx = *self.map.get(key)?;
        let expired = self.entries[idx].as_ref()?.expires_at < Instant::now();
        if expired {
            self.remove(key);
            return None;
        }
        self.detach(idx);
        self.attach_front(idx);
        self.entries[idx].as_ref().map(|e| &e.value)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let idx = self.map.remove(key)?;
        self.detach(idx);
        self.reverse[idx] = None;
        let entry = self.entries[idx].take()?;
        self.free.push(idx);
        Some(entry.value)
    }
}

pub fn build_key(key: &str, id: u64) -> u64 {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    id.hash(&mut h);
    h.finish()
}
