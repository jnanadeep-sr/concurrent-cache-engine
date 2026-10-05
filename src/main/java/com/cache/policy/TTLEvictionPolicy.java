package com.cache.policy;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Time-to-live policy. Every put (re)sets the expiry; reads do not extend it. Expiry is enforced
 * lazily through {@link #isExpired(Object)} and by a background purge. When capacity forces an
 * eviction, the entry closest to expiry is chosen (O(log n) via an ordered set).
 */
public final class TTLEvictionPolicy<K, V> implements EvictionPolicy<K, V> {

    private record Expiry<K>(long expiresAt, long seq, K key) {
    }

    private final long ttlMillis;
    private final long cleanupIntervalMillis;
    private final LongSupplier clockMillis;
    private final Map<K, Expiry<K>> entries = new HashMap<>();
    private final TreeSet<Expiry<K>> byExpiry = new TreeSet<>(
            Comparator.comparingLong((Expiry<K> e) -> e.expiresAt()).thenComparingLong(Expiry::seq));
    private long sequence = 0;
    private ScheduledExecutorService scheduler;

    public TTLEvictionPolicy(long ttlMillis, long cleanupIntervalMillis) {
        this(ttlMillis, cleanupIntervalMillis, System::currentTimeMillis);
    }

    public TTLEvictionPolicy(long ttlMillis, long cleanupIntervalMillis, LongSupplier clockMillis) {
        if (ttlMillis <= 0 || cleanupIntervalMillis <= 0) {
            throw new IllegalArgumentException("ttl and cleanup interval must be positive");
        }
        this.ttlMillis = ttlMillis;
        this.cleanupIntervalMillis = cleanupIntervalMillis;
        this.clockMillis = clockMillis;
    }

    @Override
    public void keyAccessed(K key) {
        // Reads intentionally do not extend the TTL.
    }

    @Override
    public synchronized void keyAdded(K key, V value) {
        Expiry<K> old = entries.remove(key);
        if (old != null) {
            byExpiry.remove(old);
        }
        Expiry<K> entry = new Expiry<>(clockMillis.getAsLong() + ttlMillis, sequence++, key);
        entries.put(key, entry);
        byExpiry.add(entry);
    }

    @Override
    public synchronized K evictKey() {
        Expiry<K> first = byExpiry.pollFirst();
        if (first == null) {
            return null;
        }
        entries.remove(first.key());
        return first.key();
    }

    @Override
    public synchronized void keyRemoved(K key) {
        Expiry<K> entry = entries.remove(key);
        if (entry != null) {
            byExpiry.remove(entry);
        }
    }

    @Override
    public synchronized boolean isExpired(K key) {
        Expiry<K> entry = entries.get(key);
        return entry != null && entry.expiresAt() <= clockMillis.getAsLong();
    }

    @Override
    public synchronized List<K> purgeExpired() {
        long now = clockMillis.getAsLong();
        List<K> expired = new ArrayList<>();
        while (!byExpiry.isEmpty() && byExpiry.first().expiresAt() <= now) {
            Expiry<K> entry = byExpiry.pollFirst();
            entries.remove(entry.key());
            expired.add(entry.key());
        }
        return expired;
    }

    @Override
    public synchronized void scheduleCleanup(Runnable purgeTask) {
        if (scheduler != null) {
            throw new IllegalStateException("cleanup already scheduled");
        }
        scheduler = Executors.newScheduledThreadPool(1, runnable -> {
            Thread t = new Thread(runnable, "ttl-cache-purger");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                purgeTask.run();
            } catch (RuntimeException ignored) {
                // Keep the periodic task alive; the next cycle retries.
            }
        }, cleanupIntervalMillis, cleanupIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }
}
