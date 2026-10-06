# Concurrent Cache Engine

A bounded, thread-safe in-memory key-value cache for Java 17+, with pluggable LRU, LFU, and TTL policies. The project includes JUnit 5 tests and a multithreaded throughput and latency benchmark.

## Requirements

- JDK 17 or later
- Maven 3.8 or later

Check that `java -version` and `mvn -version` report the expected installations.

## Build and test

From the repository root, compile and run the JUnit 5 test suite:

```sh
mvn test
```

The build uses Maven to download JUnit and build plugins on the first run. Test reports are written under `target/surefire-reports/`.

## Use the cache

Create a cache with a positive capacity and an eviction policy. Keys and values must be non-null; `get` returns `null` for a miss. The cache owns the supplied policy, so close the cache when finished—especially when using TTL, which starts a background cleanup scheduler.

```java
import com.cache.core.ConcurrentCacheEngine;
import com.cache.policy.LRUEvictionPolicy;

public class Example {
	public static void main(String[] args) {
		try (var cache = new ConcurrentCacheEngine<String, String>(
				1_000, new LRUEvictionPolicy<>())) {
			cache.put("user:42", "Ada");
			String name = cache.get("user:42");
			System.out.println(name);
		}
	}
}
```

The engine supports `get`, `put`, `remove`, `size`, and `clear` through `com.cache.core.Cache`. A `put` for an existing key updates its value without consuming another capacity slot.

## Eviction policies

| Policy | Behavior | Complexity |
| --- | --- | --- |
| `LRUEvictionPolicy` | Evicts the least recently accessed key. Uses a custom doubly linked list and hash map; an update also refreshes recency. | O(1) access, update, and eviction. |
| `LFUEvictionPolicy` | Evicts the least frequently accessed key; ties are broken by least-recently-used order within a frequency bucket. A new or updated key is assigned/incremented as implemented by the policy. | O(1) access and insertion in the normal case. If a manual removal empties the minimum-frequency bucket, the next eviction rescans the remaining frequency buckets. |
| `TTLEvictionPolicy` | Expires entries after a fixed TTL. `get` checks expiry lazily, and a daemon scheduled thread periodically removes expired entries. Reads do not extend the TTL; each `put` sets or refreshes it. | Ordered expiry tracking uses O(log n) insertion/removal and O(1) earliest-expiry lookup. |

TTL takes the duration and cleanup period in milliseconds:

```java
import com.cache.core.ConcurrentCacheEngine;
import com.cache.policy.TTLEvictionPolicy;

var cache = new ConcurrentCacheEngine<String, String>(
		500, new TTLEvictionPolicy<>(30_000, 1_000));
try (cache) {
	cache.put("session", "active");
}
```

The cache's `size()` counts entries until lazy access or the scheduled TTL purge removes them. When a full cache needs a capacity eviction, TTL selects the entry with the earliest expiry.

## Concurrency model

`ConcurrentCacheEngine` protects its backing map with a `ReentrantReadWriteLock`. Reads share the read lock; writes, capacity evictions, removals, clears, and TTL cleanup use the write lock. Policy implementations synchronize their own bookkeeping so access-order or frequency state stays consistent during concurrent reads. Writes and policy-driven updates can contend under high load; benchmark results depend on the machine and workload.

The engine requires a positive maximum capacity and rejects null keys and values. For TTL caches, use try-with-resources or call `close()` to stop the background scheduler.

## Run the benchmark

```sh
mvn compile exec:java -Dexec.mainClass=com.cache.CacheBenchmark
```

The benchmark runs mixed operations with 80% reads and reports aggregate operations per second plus p50, p99, and p99.9 operation latency for LRU and LFU. It uses multiple worker threads and can allocate substantial memory for per-operation latency samples; the default workload is intended for local benchmarking, not a stable cross-machine performance guarantee. In particular, a reported sub-millisecond p99 is an observation, not a service-level guarantee.

## Project layout

```text
src/
├── main/java/com/cache/
│   ├── CacheBenchmark.java
│   ├── core/
│   │   ├── Cache.java
│   │   └── ConcurrentCacheEngine.java
│   └── policy/
│       ├── EvictionPolicy.java
│       ├── LFUEvictionPolicy.java
│       ├── LRUEvictionPolicy.java
│       └── TTLEvictionPolicy.java
└── test/java/com/cache/
	└── ConcurrentCacheEngineTest.java
```

`mvn clean` removes Maven's generated `target/` directory. It is excluded from version control by `.gitignore`.