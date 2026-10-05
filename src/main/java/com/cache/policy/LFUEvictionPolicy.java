package com.cache.policy;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * LFU with frequency buckets. Each bucket is a LinkedHashSet in insertion order, so ties on the
 * lowest frequency evict the least recently used key. Add, access and evict are O(1); only after an
 * external removal empties the minimum bucket does the next evict rescan the distinct frequencies.
 */
public final class LFUEvictionPolicy<K, V> implements EvictionPolicy<K, V> {

    private final Map<K, Integer> frequencies = new HashMap<>();
    private final Map<Integer, LinkedHashSet<K>> buckets = new HashMap<>();
    private int minFrequency = 0;

    @Override
    public synchronized void keyAccessed(K key) {
        Integer freq = frequencies.get(key);
        if (freq != null) {
            promote(key, freq);
        }
    }

    @Override
    public synchronized void keyAdded(K key, V value) {
        Integer freq = frequencies.get(key);
        if (freq != null) {
            promote(key, freq);
            return;
        }
        frequencies.put(key, 1);
        buckets.computeIfAbsent(1, f -> new LinkedHashSet<>()).add(key);
        minFrequency = 1;
    }

    @Override
    public synchronized K evictKey() {
        if (frequencies.isEmpty()) {
            return null;
        }
        if (!buckets.containsKey(minFrequency)) {
            minFrequency = Integer.MAX_VALUE;
            for (int f : buckets.keySet()) {
                minFrequency = Math.min(minFrequency, f);
            }
        }
        LinkedHashSet<K> bucket = buckets.get(minFrequency);
        Iterator<K> it = bucket.iterator();
        K victim = it.next();
        it.remove();
        if (bucket.isEmpty()) {
            buckets.remove(minFrequency);
        }
        frequencies.remove(victim);
        return victim;
    }

    @Override
    public synchronized void keyRemoved(K key) {
        Integer freq = frequencies.remove(key);
        if (freq != null) {
            removeFromBucket(key, freq);
        }
    }

    private void promote(K key, int freq) {
        removeFromBucket(key, freq);
        frequencies.put(key, freq + 1);
        buckets.computeIfAbsent(freq + 1, f -> new LinkedHashSet<>()).add(key);
        if (freq == minFrequency && !buckets.containsKey(freq)) {
            minFrequency = freq + 1;
        }
    }

    private void removeFromBucket(K key, int freq) {
        LinkedHashSet<K> bucket = buckets.get(freq);
        bucket.remove(key);
        if (bucket.isEmpty()) {
            buckets.remove(freq);
        }
    }
}
