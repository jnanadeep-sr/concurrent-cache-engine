package com.cache;

import com.cache.core.Cache;
import com.cache.core.ConcurrentCacheEngine;
import com.cache.policy.EvictionPolicy;
import com.cache.policy.LFUEvictionPolicy;
import com.cache.policy.LRUEvictionPolicy;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

public final class CacheBenchmark {

    private static final int THREADS = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);
    private static final int OPS_PER_THREAD = 500_000;
    private static final int CAPACITY = 10_000;
    private static final int KEY_SPACE = 50_000;
    private static final int READ_PERCENT = 80;

    private CacheBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        System.out.printf("threads=%d opsPerThread=%d capacity=%d keySpace=%d reads=%d%%%n",
                THREADS, OPS_PER_THREAD, CAPACITY, KEY_SPACE, READ_PERCENT);
        run("LRU", new LRUEvictionPolicy<>());
        run("LFU", new LFUEvictionPolicy<>());
    }

    private static void run(String name, EvictionPolicy<Integer, Integer> policy) throws Exception {
        try (ConcurrentCacheEngine<Integer, Integer> cache = new ConcurrentCacheEngine<>(CAPACITY, policy)) {
            for (int i = 0; i < CAPACITY; i++) {
                cache.put(i, i);
            }
            workload(cache, OPS_PER_THREAD / 5, new long[THREADS][0]); // warm-up

            long[][] latencies = new long[THREADS][];
            long wallNanos = workload(cache, OPS_PER_THREAD, latencies);

            long[] all = new long[THREADS * OPS_PER_THREAD];
            int pos = 0;
            for (long[] l : latencies) {
                System.arraycopy(l, 0, all, pos, l.length);
                pos += l.length;
            }
            Arrays.sort(all);

            double totalOps = all.length;
            double throughput = totalOps / (wallNanos / 1e9);
            long p50 = all[(int) (all.length * 0.50)];
            long p99 = all[(int) (all.length * 0.99)];
            long p999 = all[(int) (all.length * 0.999)];

            System.out.printf("[%s] throughput=%,.0f ops/sec  p50=%.3f us  p99=%.3f us (%.4f ms)  p99.9=%.3f us  %s%n",
                    name, throughput, p50 / 1e3, p99 / 1e3, p99 / 1e6, p999 / 1e3,
                    p99 < 1_000_000 ? "p99 < 1ms: OK" : "p99 >= 1ms");
        }
    }

    /** Runs the mixed workload on all threads and returns wall-clock nanos; fills per-op latencies. */
    private static long workload(Cache<Integer, Integer> cache, int opsPerThread, long[][] latencies)
            throws InterruptedException {
        CountDownLatch ready = new CountDownLatch(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREADS);

        for (int t = 0; t < THREADS; t++) {
            final int idx = t;
            long[] samples = new long[opsPerThread];
            latencies[idx] = samples;
            Thread worker = new Thread(() -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                ready.countDown();
                try {
                    start.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        int key = rnd.nextInt(KEY_SPACE);
                        boolean read = rnd.nextInt(100) < READ_PERCENT;
                        long t0 = System.nanoTime();
                        if (read) {
                            cache.get(key);
                        } else {
                            cache.put(key, key);
                        }
                        samples[i] = System.nanoTime() - t0;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }, "bench-" + t);
            worker.start();
        }

        ready.await();
        long begin = System.nanoTime();
        start.countDown();
        done.await();
        return System.nanoTime() - begin;
    }
}
