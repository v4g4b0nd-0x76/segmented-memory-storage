# mem-stream

> **Heads up:** this project is a learning exercise for getting comfortable with Rust — not meant for production use.

A toy in-memory database written in Rust with a custom TCP server, three different storage primitives, and a lightweight client that wraps everything behind an HTTP API.

---

## Server

The core of the project. Everything lives in memory; there's no persistence by default beyond the AOF log described below.

### Segmented log (`SegLog`)

The storage engine allocates memory in fixed-size 64 KB segments aligned to page boundaries. New segments are added on demand rather than growing a single contiguous buffer, which keeps writes non-overlapping (think DMA-style copies via `ptr::copy_nonoverlapping`). Append is O(1) and reads are O(log N) thanks to a `BTreeMap` index of entry IDs. Groups let you namespace multiple independent logs.

### Key-value store (`KV`)

A straightforward string-keyed store with optional TTL per entry. It works a bit like Redis in that you can have multiple isolated databases by name. Reads go through an LRU cache, which helps a lot on read-heavy workloads — though the tradeoff is that replaying the AOF on restart is slower.

### Segmented list (`SegList`)

A stack-like structure with `push`, `push_range`, `pop`, `pop_count`, `pop_range`, and `flush`. Backed by the same segmented memory layout as the log.

### Protocol & TCP server

Rather than using an off-the-shelf protocol, the server speaks its own binary format over TCP. Each frame is length-prefixed (4-byte LE header) and the commands are encoded as fixed-size byte sequences with a single opcode byte — roughly similar to how Redis' RESP works but at a lower level. Tokio drives the async networking side.

### AOF (append-only file)

Every write is serialized as a JSON entry and appended to a log file. On startup the DB replays this log to rebuild state. The same AOF stream is also used for replication — followers can catch up by consuming entries from the leader's log.

### Pipeline

There's a basic batching layer that lets you group multiple KV, list, or log operations into a single atomic job. Jobs are queued by command count (smaller batches go first) and a resource-based lock manager allows non-conflicting pipelines to run in parallel.

### High availability

A basic leader/follower setup using the same TCP protocol. Followers register with the leader and receive AOF entries as they come in. Heartbeats are used to detect dead followers.

---

## Client

The client side has two pieces: a connection pool that manages raw TCP connections to the server using the same binary protocol, and an HTTP layer (built on top) that exposes the same operations as plain REST endpoints. The HTTP API makes it easier to poke at the server without writing custom protocol handling.
