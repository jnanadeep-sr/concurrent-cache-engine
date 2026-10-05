package com.cache;

import com.cache.core.ConcurrentCacheEngine;
import com.cache.policy.LFUEvictionPolicy;
import com.cache.policy.LRUEvictionPolicy;
import com.cache.policy.TTLEvictionPolicy;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrentCacheEngineTest {

    @Test
    void basicCrudAndMisses() {
        try (var cache = new ConcurrentCacheEngine<String, Integer>(3, new LRUEvictionPolicy<>())) {
            assertNull(cache.get("missing"));
            cache.put("a", 1);
            cache.put("b", 2);
            assertEquals(1, cache.get("a"));
            assertEquals(2, cache.size());

            cache.put("a", 10);
            assertEquals(10, cache.get("a"));
            assertEquals(2, cache.size());

            cache.remove("a");
            assertNull(cache.get("a"));
            cache.remove("never-there");
            assertEquals(1, cache.size());

            cache.clear();
            assertEquals(0, cache.size());
            assertNull(cache.get("b"));

            assertThrows(NullPointerException.class, () -> cache.put(null, 1));
            assertThrows(NullPointerException.class, () -> cache.put("k", null));
            assertThrows(IllegalArgumentException.class,
                    () -> new ConcurrentCacheEngine<String, Integer>(0, new LRUEvictionPolicy<>()));
        }
    }

    @Test
    void lruEvictsLeastRecentlyUsed() {
        try (var cache = new ConcurrentCacheEngine<String, Integer>(3, new LRUEvictionPolicy<>())) {
            cache.put("a", 1);
            cache.put("b", 2);
            cache.put("c", 3);
            cache.get("a");          // order (old -> new): b, c, a
            cache.put("d", 4);       // evicts b
            assertNull(cache.get("b"));
            assertEquals(1, cache.get("a"));
            assertEquals(3, cache.get("c"));
            assertEquals(4, cache.get("d"));

            cache.put("c", 30);      // update refreshes c; order: a, d, c
            cache.put("e", 5);       // evicts a
            assertNull(cache.get("a"));
            assertEquals(3, cache.size());
        }
    }

    @Test
    void lruPolicyEvictionSequence() {
        var policy = new LRUEvictionPolicy<String, Integer>();
        assertNull(policy.evictKey());
        policy.keyAdded("a", 1);
        policy.keyAdded("b", 2);
        policy.keyAdded("c", 3);
        policy.keyAccessed("a");
        policy.keyRemoved("c");
        assertEquals("b", policy.evictKey());
        assertEquals("a", policy.evictKey());
        assertNull(policy.evictKey());
    }

    @Test
    void lfuEvictsLeastFrequentlyUsedWithLruTieBreak() {
        try (var cache = new ConcurrentCacheEngine<String, Integer>(3, new LFUEvictionPolicy<>())) {
            cache.put("a", 1);
            cache.put("b", 2);
            cache.put("c", 3);
            cache.get("a");
            cache.get("a");
            cache.get("b");          // freq: a=3, b=2, c=1
            cache.put("d", 4);       // evicts c
            assertNull(cache.get("c"));

            // d=1 is now the minimum, so the next insert evicts it.
            cache.put("e", 5);
            assertNull(cache.get("d"));
            assertEquals(1, cache.get("a"));
            assertEquals(2, cache.get("b"));
            assertEquals(5, cache.get("e"));
        }
    }

    @Test
    void lfuTieBreaksByInsertionOrderAndSurvivesExternalRemoval() {
        var policy = new LFUEvictionPolicy<String, Integer>();
        policy.keyAdded("a", 1);
        policy.keyAdded("b", 2);
        policy.keyAdded("c", 3);
        assertEquals("a", policy.evictKey());

        policy.keyAccessed("b");
        policy.keyAccessed("c");
        policy.keyAccessed("c");        // b=2, c=3
        policy.keyAdded("d", 4);        // d=1, min
        policy.keyRemoved("d");         // min bucket vanishes externally
        assertEquals("b", policy.evictKey());
        assertEquals("c", policy.evictKey());
        assertNull(policy.evictKey());
    }

    @Test
    void ttlLazyExpirationOnGet() throws InterruptedException {
        // Cleanup interval is huge, so only the lazy path on get() can remove the entry.
        try (var cache = new ConcurrentCacheEngine<String, String>(
                10, new TTLEvictionPolicy<>(100, 3_600_000))) {
            cache.put("k", "v");
            assertEquals("v", cache.get("k"));
            Thread.sleep(200);
            assertEquals(1, cache.size());
            assertNull(cache.get("k"));
            assertEquals(0, cache.size());
        }
    }

    @Test
    void ttlLazyExpirationWithFakeClock() {
        long[] now = {1_000};
        try (var cache = new ConcurrentCacheEngine<String, String>(
                10, new TTLEvictionPolicy<>(50, 3_600_000, () -> now[0]))) {
            cache.put("k", "v");
            now[0] += 49;
            assertEquals("v", cache.get("k"));
            now[0] += 1;
            assertNull(cache.get("k"));
        }
    }

    @Test
    void ttlBackgroundThreadPurgesWithoutAccess() throws InterruptedException {
        try (var cache = new ConcurrentCacheEngine<String, String>(
                10, new TTLEvictionPolicy<>(50, 20))) {
            cache.put("a", "1");
            cache.put("b", "2");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (cache.size() > 0 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(0, cache.size(), "background purge should have removed expired keys");
        }
    }

    @Test
    void ttlCapacityEvictsEarliestExpiry() {
        long[] now = {0};
        try (var cache = new ConcurrentCacheEngine<String, String>(
                2, new TTLEvictionPolicy<>(100, 3_600_000, () -> now[0]))) {
            cache.put("a", "1");
            now[0] = 10;
            cache.put("b", "2");
            now[0] = 20;
            cache.put("c", "3");     // evicts a (earliest expiry)
            assertNull(cache.get("a"));
            assertEquals("2", cache.get("b"));
            assertEquals("3", cache.get("c"));
        }
    }

    @Test
    void concurrentReadsAndWritesStayConsistent() throws Exception {
        runConcurrencyScenario(new LRUEvictionPolicy<>());
        runConcurrencyScenario(new LFUEvictionPolicy<>());
        runConcurrencyScenario(new TTLEvictionPolicy<>(5_000, 10));
    }

    private void runConcurrencyScenario(com.cache.policy.EvictionPolicy<Integer, String> policy)
            throws Exception {
        final int threads = 64;
        final int opsPerThread = 5_000;
        final int capacity = 100;
        final int keySpace = 500;

        try (var cache = new ConcurrentCacheEngine<Integer, String>(capacity, policy)) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            List<Future<Integer>> results = new ArrayList<>();

            for (int t = 0; t < threads; t++) {
                results.add(pool.submit(() -> {
                    int corrupted = 0;
                    ready.countDown();
                    try {
                        start.await();
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();
                        for (int i = 0; i < opsPerThread; i++) {
                            int key = rnd.nextInt(keySpace);
                            int op = rnd.nextInt(10);
                            if (op < 5) {
                                String v = cache.get(key);
                                if (v != null && !v.equals("v" + key)) {
                                    corrupted++;
                                }
                            } else if (op < 9) {
                                cache.put(key, "v" + key);
                            } else {
                                cache.remove(key);
                            }
                            if (cache.size() > capacity) {
                                corrupted++;
                            }
                        }
                    } finally {
                        done.countDown();
                    }
                    return corrupted;
                }));
            }

            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            assertTrue(done.await(60, TimeUnit.SECONDS), "workers did not finish (possible deadlock)");
            pool.shutdown();

            for (Future<Integer> f : results) {
                assertEquals(0, f.get(), "corruption or capacity violation observed");
            }
            assertTrue(cache.size() <= capacity);

            // Policy and store must still agree: filling beyond capacity must keep evicting cleanly.
            for (int i = 0; i < capacity * 3; i++) {
                cache.put(10_000 + i, "v" + (10_000 + i));
                assertTrue(cache.size() <= capacity);
            }
            assertEquals(capacity, cache.size());
        }
    }
}
