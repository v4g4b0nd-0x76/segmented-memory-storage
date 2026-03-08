# Segmented Memory Storage

## Server

Separate memory in fixed size segments and add new segments when current ones is full
by DMA we increase the performance of writing O(1) by copy data in non overlapping manner into segment pointers and for reading by keeping a BtreeMap of segment indexes we achieve O(log N);

We can have many groups that each would have this segments and write to groups;

Custom tcp frame encoder and decoder with custom fixed sized entries for commands(similar to REPL in redis);

Tokio simple Tcp connection for p2p communication;

LRU in group wrapper level for better performance and less segment access.

Key Value (KV) storage with ttl option and different database isolated access like redis.
KV wrapper has LRU which improves read time in heavy read workload(the write time - reload AOF is downside)

Seg List which is like redis stack to push, push_range, pop, pop_count, pop_range and flush features

there is a wrapper around all data sets that has an aof and apply commands done in that aof in order to recreate data
the aof file can be used as change log for replica follower catchups

Pipeline: A concurrent command batching system that groups DB operations (KV, List, Log) into atomic jobs, queues them by operation count (shorter jobs run first), and uses a resource-based lock manager to safely execute non-conflicting pipelines in parallel.

## Client

Custom connection pool for connections to server using its own protocol

Http endpoints for easier access to server with no need to handle custom registries.
