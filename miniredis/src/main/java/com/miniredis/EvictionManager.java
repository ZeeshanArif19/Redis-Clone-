package com.miniredis;

import java.util.ArrayList;
import java.util.Set;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class EvictionManager {

    public enum EvictionPolicy {
        NOEVICTION, // returns an OOM error on writes when limit is reached
        ALLKEYS_LRU, // evicts least recently used key among all keys
        VOLATILE_LRU // evicts least recently used key among keys with a TTL
    }

    private static final int MAX_KEYS = 10000;
    private static volatile EvictionPolicy currentPolicy = EvictionPolicy.ALLKEYS_LRU;
    private static final ConcurrentHashMap<String, Long> lastAccessed = new ConcurrentHashMap<>();

    // record key access timestamps
    public static void touchKey(String key) {
        lastAccessed.put(key, System.currentTimeMillis());
    }

    // Clean up metadata when a key is deleted
    public static void removeKey(String key) {
        lastAccessed.remove(key);
    }

    // Call before executing memory-expanding write cmds
    public static boolean evictIfNecessary(ConcurrentHashMap<String, ?> dataStore,
            ConcurrentHashMap<String, Long> ttlstore) {

        if (dataStore.size() < MAX_KEYS)
            return true; // memory space available
        if (currentPolicy == EvictionPolicy.NOEVICTION)
            return false; // cannot write, memory full

        // Determine candidate pool based on policy
        Set<String> candidateKeys;
        if (currentPolicy == EvictionPolicy.VOLATILE_LRU) {
            candidateKeys = ttlstore.keySet();
            if (candidateKeys.isEmpty()) {
                return false; // no volatile keys available
            }
        } else {
            candidateKeys = dataStore.keySet();
        }

        // Redis Approximate LRU
        int sampleSize = 5;
        // Reservoir sampling avoids always examining the same first keys from
        // ConcurrentHashMap iteration order.
        List<String> samples = new ArrayList<>(sampleSize);
        int seen = 0;
        for (String key : candidateKeys) {
            seen++;
            if (samples.size() < sampleSize) {
                samples.add(key);
            } else if (ThreadLocalRandom.current().nextInt(seen) == 0) {
                samples.set(ThreadLocalRandom.current().nextInt(sampleSize), key);
            }
        }
        // find the key with the oldest(smallest) lastAccessed timestamp
        String victimKey = null;
        long oldestTimeStamp = Long.MAX_VALUE;

        for (String key : samples) {
            long accessTime = lastAccessed.getOrDefault(key, 0L);
            if (accessTime < oldestTimeStamp) {
                oldestTimeStamp = accessTime;
                victimKey = key;
            }
        }
        if (victimKey != null) {
            dataStore.remove(victimKey);
            ttlstore.remove(victimKey);
            lastAccessed.remove(victimKey);
            System.out.println("[EVICTION] Evicted key: " + victimKey + " under policy " + currentPolicy);
            return true;
        }
        return false;
    }

    public static void setPolicy(EvictionPolicy policy) {
        currentPolicy = policy;
    }

    public static EvictionPolicy getPolicy() {
        return currentPolicy;
    }
}
