# Concurrent Cache Engine

A high-throughput, thread-safe in-memory key-value cache engine in Java 17 featuring pluggable eviction policies (\(O(1)\) complexity) and fine-grained locking.

## Key Features

* **Pluggable Eviction Strategies:** Implements the Strategy Pattern to seamlessly swap eviction policies at runtime (`LRU`, `LFU`, `TTL`).