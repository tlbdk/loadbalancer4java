package dk.nversion;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

// TODO: http://www.stoyanr.com/2012/12/evictor-java-concurrent-map-with-timed.html

public class CompletableFutureCache<K, V> {
    Map<K, V> map = new ConcurrentHashMap<K,V>();

    public CompletableFutureCache(TriConsumer<K, V, CompletableFutureCache<K, V>> cache) {

    }

    public void put(K key, V value, int expires, TimeUnit expiresTimeUnit) {

    }

    public V get(K key, V defaultValue) {
        return null;
    }

    public CompletableFuture<V> wrap(K key, CompletableFuture<V> completableFutureValue) {
        return completableFutureValue;
    }
}
