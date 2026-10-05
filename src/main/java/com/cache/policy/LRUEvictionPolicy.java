package com.cache.policy;

import java.util.HashMap;
import java.util.Map;

/** O(1) LRU using a hand-rolled doubly linked list (head = most recent) plus a HashMap. */
public final class LRUEvictionPolicy<K, V> implements EvictionPolicy<K, V> {

    private static final class Node<K> {
        final K key;
        Node<K> prev;
        Node<K> next;

        Node(K key) {
            this.key = key;
        }
    }

    private final Map<K, Node<K>> nodes = new HashMap<>();
    private final Node<K> head = new Node<>(null);
    private final Node<K> tail = new Node<>(null);

    public LRUEvictionPolicy() {
        head.next = tail;
        tail.prev = head;
    }

    @Override
    public synchronized void keyAccessed(K key) {
        Node<K> node = nodes.get(key);
        if (node != null) {
            unlink(node);
            linkFirst(node);
        }
    }

    @Override
    public synchronized void keyAdded(K key, V value) {
        Node<K> node = nodes.get(key);
        if (node == null) {
            node = new Node<>(key);
            nodes.put(key, node);
        } else {
            unlink(node);
        }
        linkFirst(node);
    }

    @Override
    public synchronized K evictKey() {
        Node<K> lru = tail.prev;
        if (lru == head) {
            return null;
        }
        unlink(lru);
        nodes.remove(lru.key);
        return lru.key;
    }

    @Override
    public synchronized void keyRemoved(K key) {
        Node<K> node = nodes.remove(key);
        if (node != null) {
            unlink(node);
        }
    }

    private void unlink(Node<K> node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        node.prev = null;
        node.next = null;
    }

    private void linkFirst(Node<K> node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
    }
}
