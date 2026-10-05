package com.cache.core;

import com.cache.policy.EvictionPolicy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Bounded cache guarded by a read/write lock. Reads share the read lock; the store is only mutated
 * under the write lock, so capacity can never be exceeded. Policy bookkeeping done by readers is
 * serialized inside the policy itself.
 */
public final class ConcurrentCacheEngine<K, V> implements Cache<K, V>, AutoCloseable {

    private final int capacity;
    private final EvictionPolicy<K, V> policy;
    private final Map<K, V> store;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public ConcurrentCacheEngine(int capacity, EvictionPolicy<K, V> policy) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.policy = Objects.requireNonNull(policy, "policy");
        this.store = new HashMap<>(Math.max(16, (int) (capacity / 0.75f) + 1));
        policy.scheduleCleanup(this::purgeExpired);
    }

    @Override
    public V get(K key) {
        Objects.requireNonNull(key, "key");
        readLock.lock();
        try {
            V value = store.get(key);
            if (value == null) {
                return null;
            }
            if (!policy.isExpired(key)) {
                policy.keyAccessed(key);
                return value;
            }
        } finally {
            readLock.unlock();
        }
        return evictIfStillExpired(key);
    }

    /** Lazy expiration: upgrades to the write lock and re-validates before removing. */
    private V evictIfStillExpired(K key) {
        writeLock.lock();
        try {
            V value = store.get(key);
            if (value == null) {
                return null;
            }
            if (policy.isExpired(key)) {
                store.remove(key);
                policy.keyRemoved(key);
                return null;
            }
            policy.keyAccessed(key);
            return value;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void put(K key, V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        writeLock.lock();
        try {
            if (!store.containsKey(key) && store.size() >= capacity) {
                K victim = policy.evictKey();
                if (victim == null) {
                    throw new IllegalStateException("cache is full but policy returned no victim");
                }
                store.remove(victim);
            }
            store.put(key, value);
            policy.keyAdded(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void remove(K key) {
        Objects.requireNonNull(key, "key");
        writeLock.lock();
        try {
            if (store.remove(key) != null) {
                policy.keyRemoved(key);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /** Entries that expired but were not yet purged are still counted. */
    @Override
    public int size() {
        readLock.lock();
        try {
            return store.size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void clear() {
        writeLock.lock();
        try {
            List<K> keys = new ArrayList<>(store.keySet());
            store.clear();
            keys.forEach(policy::keyRemoved);
        } finally {
            writeLock.unlock();
        }
    }

    private void purgeExpired() {
        writeLock.lock();
        try {
            for (K key : policy.purgeExpired()) {
                store.remove(key);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() {
        policy.close();
    }
}
