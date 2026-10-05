package com.cache.policy;

/**
 * Strategy deciding which key to evict. Implementations must be internally thread-safe because
 * {@link #keyAccessed(Object)} may be invoked concurrently by readers holding only a read lock.
 */
public interface EvictionPolicy<K, V> extends AutoCloseable {

    /** Records a read of an existing key. */
    void keyAccessed(K key);

    /** Records an insert; if the key is already tracked it is treated as an update. */
    void keyAdded(K key, V value);

    /** Selects the victim, stops tracking it and returns it; {@code null} if nothing is tracked. */
    K evictKey();

    /** Stops tracking the key; no-op if unknown. */
    void keyRemoved(K key);

    /** Whether the key has expired and must be treated as a miss. */
    default boolean isExpired(K key) {
        return false;
    }

    /** Stops tracking and returns every expired key. */
    default java.util.List<K> purgeExpired() {
        return java.util.List.of();
    }

    /** Lets time-based policies run {@code purgeTask} periodically on their own threads. */
    default void scheduleCleanup(Runnable purgeTask) {
    }

    @Override
    default void close() {
    }
}
