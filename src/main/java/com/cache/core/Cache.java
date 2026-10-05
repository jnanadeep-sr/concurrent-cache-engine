package com.cache.core;

public interface Cache<K, V> {

    /** Returns the cached value or {@code null} on a miss. */
    V get(K key);

    void put(K key, V value);

    void remove(K key);

    int size();

    void clear();
}
