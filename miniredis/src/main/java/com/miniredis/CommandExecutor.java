package com.miniredis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.miniredis.RedisObject.DataType;

public class CommandExecutor {
    private final ConcurrentHashMap<String, RedisObject> dataStore;
    private final ConcurrentHashMap<String, Long> ttlstore;

    public CommandExecutor(ConcurrentHashMap<String, RedisObject> dataStore, ConcurrentHashMap<String, Long> ttlstore) {
        this.dataStore = dataStore;
        this.ttlstore = ttlstore;
    }

    public byte[] execute(List<String> commandTokens) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String cmd = commandTokens.get(0).toUpperCase();

        switch (cmd) {
            case "PING":
                RespWriter.writeSimpleString(out, "PONG");
                break;

            case "SET": {
                if (commandTokens.size() < 3) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'SET'");
                } else {
                    String key = commandTokens.get(1);
                    String val = commandTokens.get(2);
                    // Updating an existing key does not increase the key count, so evict only for a
                    // new key.
                    if (!dataStore.containsKey(key) && !EvictionManager.evictIfNecessary(dataStore, ttlstore)) {
                        RespWriter.writeError(out, "OOM command not allowed when used memory > maxmemory");
                        break;
                    }
                    dataStore.put(key, new RedisObject(DataType.STRING, val));
                    ttlstore.remove(key); // overwriting key cancels previous TTL

                    EvictionManager.touchKey(key);
                    RespWriter.writeSimpleString(out, "OK");
                }
                break;
            }
            case "GET": {
                if (commandTokens.size() < 2) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'GET'");
                } else {
                    String key = commandTokens.get(1);
                    RedisObject obj = dataStore.get(key);

                    if (obj != null && obj.getType() != DataType.STRING) {
                        RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                        break;
                    }
                    if (isExpired(key)) {
                        RespWriter.writeBulkString(out, null);
                    } else {
                        String val = (obj == null) ? null : (String) obj.getValue();
                        if (val != null) {
                            EvictionManager.touchKey(key);
                        }
                        RespWriter.writeBulkString(out, val);
                    }
                }
                break;
            }
            case "HSET": {
                if (commandTokens.size() < 4) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'HSET'");
                    break;
                } else {
                    String key = commandTokens.get(1);
                    String field = commandTokens.get(2);
                    String value = commandTokens.get(3);

                    RedisObject obj = dataStore.get(key);
                    if (obj != null && obj.getType() != DataType.HASH) {
                        RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                        break;
                    }

                    Map<String, String> map;
                    if (obj == null) {
                        // Evict only when this command creates a new key; updates should not evict
                        // another key.
                        if (!EvictionManager.evictIfNecessary(dataStore, ttlstore)) {
                            RespWriter.writeError(out, "OOM command not allowed when used memory > maxmemory");
                            break;
                        }
                        map = new HashMap<>();
                        dataStore.put(key, new RedisObject(DataType.HASH, map));
                    } else {
                        map = obj.getHash();
                    }

                    boolean isNewField = !map.containsKey(field);
                    map.put(field, value);
                    RespWriter.writeInteger(out, isNewField ? 1 : 0);
                }
                break;
            }
            case "HGET": {
                if (commandTokens.size() < 3) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'HGET'");
                    break;
                } else {
                    String key = commandTokens.get(1);
                    String field = commandTokens.get(2);

                    RedisObject obj = dataStore.get(key);
                    if (obj != null && obj.getType() != DataType.HASH) {
                        RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                        break;
                    }

                    if (obj == null || isExpired(key)) {
                        RespWriter.writeBulkString(out, null);
                    } else {
                        Map<String, String> map = obj.getHash();
                        String val = map.get(field);
                        if (val != null)
                            EvictionManager.touchKey(key);
                        RespWriter.writeBulkString(out, val);
                    }
                }
                break;
            }
            case "HGETALL": {
                if (commandTokens.size() < 2) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'HGETALL'");
                } else {
                    String key = commandTokens.get(1);
                    RedisObject obj = dataStore.get(key);
                    if (obj != null && obj.getType() != DataType.HASH) {
                        RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                        break;
                    }

                    if (obj == null || isExpired(key)) {
                        RespWriter.writeArrayHeader(out, 0);
                    } else {
                        Map<String, String> map = obj.getHash();
                        EvictionManager.touchKey(key);

                        RespWriter.writeArrayHeader(out, map.size() * 2); // HGETALL returns field1, val1, field2, val2
                                                                          // as a flat RESP array
                        for (Map.Entry<String, String> entry : map.entrySet()) {
                            RespWriter.writeBulkString(out, entry.getKey());
                            RespWriter.writeBulkString(out, entry.getValue());
                        }
                    }
                }
                break;
            }
            case "HDEL": {
                if (commandTokens.size() < 3) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'HDEL'");
                } else {
                    String key = commandTokens.get(1);
                    RedisObject obj = dataStore.get(key);
                    if (obj != null && obj.getType() != DataType.HASH) {
                        RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                        break;
                    }

                    if (obj == null || isExpired(key)) {
                        RespWriter.writeInteger(out, 0);
                    } else {
                        Map<String, String> map = obj.getHash();
                        int removedCount = 0;
                        for (int i = 2; i < commandTokens.size(); i++) {
                            if (map.remove(commandTokens.get(i)) != null)
                                removedCount++;
                        }
                        // auto-delete empty hashmap
                        if (map.isEmpty()) {
                            dataStore.remove(key);
                            ttlstore.remove(key);
                            EvictionManager.removeKey(key);
                        }
                        RespWriter.writeInteger(out, removedCount);
                    }
                }
                break;
            }
            case "RPUSH": {
                if (commandTokens.size() < 3) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'RPUSH'");
                    break;
                } else {
                    String key = commandTokens.get(1);
                    RedisObject obj = dataStore.get(key);
                    if (obj != null && obj.getType() != DataType.LIST) {
                        RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                        break;
                    }

                    Deque<String> list;
                    if (obj == null) {
                        // Evict only when this command creates a new key; updates should not evict
                        // another key.
                        if (!EvictionManager.evictIfNecessary(dataStore, ttlstore)) {
                            RespWriter.writeError(out, "OOM command not allowed when used memory > 'maxmemory'");
                            break;
                        }
                        list = new ArrayDeque<>();
                        dataStore.put(key, new RedisObject(DataType.LIST, list));
                    } else {
                        list = obj.getList();
                    }
                    for (int i = 2; i < commandTokens.size(); i++) {
                        list.addLast(commandTokens.get(i));
                    }

                    EvictionManager.touchKey(key);
                    RespWriter.writeInteger(out, list.size());
                    break;
                }
            }
            case "LPUSH": {
                if (commandTokens.size() < 3) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'LPUSH'");
                    break;
                } else {
                    String key = commandTokens.get(1);
                    RedisObject obj = dataStore.get(key);

                    if (obj != null && obj.getType() != DataType.LIST) {
                        RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                        break;
                    }

                    Deque<String> list;
                    if (obj == null) {
                        // Evict only when this command creates a new key; updates should not evict
                        // another key.
                        if (!EvictionManager.evictIfNecessary(dataStore, ttlstore)) {
                            RespWriter.writeError(out, "OOM command not allowed when used memory > 'maxmemory'");
                            break;
                        }
                        list = new ArrayDeque<>();
                        dataStore.put(key, new RedisObject(DataType.LIST, list));
                    } else {
                        list = obj.getList();
                    }

                    for (int i = 2; i < commandTokens.size(); i++) {
                        list.addFirst(commandTokens.get(i));
                    }
                    EvictionManager.touchKey(key);
                    RespWriter.writeInteger(out, list.size());
                }
                break;
            }

            case "LPOP":
            case "RPOP": {
                if (commandTokens.size() < 2) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for '" + cmd + "'");
                    break;
                }

                String key = commandTokens.get(1);
                RedisObject obj = dataStore.get(key);
                if (obj != null && obj.getType() != DataType.LIST) {
                    RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    break;
                }
                if (obj == null || isExpired(key)) {
                    RespWriter.writeBulkString(out, null);
                    break;
                } else {
                    Deque<String> list = obj.getList();
                    String popped = (cmd.equals("LPOP")) ? list.pollFirst() : list.pollLast();

                    if (popped != null) {
                        EvictionManager.touchKey(key);
                    }

                    if (list.isEmpty()) {
                        dataStore.remove(key);
                        ttlstore.remove(key);
                        EvictionManager.removeKey(key);
                    }
                    RespWriter.writeBulkString(out, popped);
                }
                break;
            }
            case "LRANGE": {
                if (commandTokens.size() < 4) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'LRANGE'");
                    break;
                }
                String key = commandTokens.get(1);
                RedisObject obj = dataStore.get(key);
                if (obj != null && obj.getType() != DataType.LIST) {
                    RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    break;
                }

                if (obj == null || isExpired(key)) {
                    RespWriter.writeArrayHeader(out, 0);
                    break;
                } else {
                    try {
                        int start = Integer.parseInt(commandTokens.get(2));
                        int stop = Integer.parseInt(commandTokens.get(3));

                        List<String> list = new ArrayList<>(obj.getList());
                        int size = list.size();

                        // normalize negative indices for redis (-1=last element)
                        if (start < 0)
                            start = size + start;
                        if (stop < 0)
                            stop = size + stop;

                        start = Math.max(0, start);
                        stop = Math.min(size - 1, stop);

                        if (start > stop || start >= size) {
                            RespWriter.writeArrayHeader(out, 0);
                        } else {
                            List<String> sublist = list.subList(start, stop + 1);
                            EvictionManager.touchKey(key);
                            RespWriter.writeArray(out, sublist);
                        }
                    } catch (NumberFormatException e) {
                        RespWriter.writeError(out, "ERR value is not an integer or out of range");
                    }
                }
                break;
            }
            case "SADD": {
                if (commandTokens.size() < 3) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'SADD'");
                    break;
                } else {
                    String key = commandTokens.get(1);
                    RedisObject obj = dataStore.get(key);

                    if (obj != null && obj.getType() != DataType.SET) {
                        RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                        break;
                    }

                    Set<String> set;
                    if (obj == null) {
                        // Evict only when this command creates a new key; updates should not evict
                        // another key.
                        if (!EvictionManager.evictIfNecessary(dataStore, ttlstore)) {
                            RespWriter.writeError(out, "OOM command not allowed when used memory > 'maxmemory'");
                            break;
                        }
                        set = new HashSet<>();
                        dataStore.put(key, new RedisObject(DataType.SET, set));
                    } else {
                        set = obj.getSet();
                    }

                    int addedCount = 0;
                    for (int i = 2; i < commandTokens.size(); i++) {
                        if (set.add(commandTokens.get(i))) {
                            addedCount++;
                        }
                    }
                    EvictionManager.touchKey(key);
                    RespWriter.writeInteger(out, addedCount);
                }
                break;
            }
            case "SMEMBERS": {
                if (commandTokens.size() < 2) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'SMEMBERS'");
                    break;
                }
                String key = commandTokens.get(1);
                RedisObject obj = dataStore.get(key);
                if (obj != null && obj.getType() != DataType.SET) {
                    RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    break;
                }

                if (obj == null || isExpired(key)) {
                    RespWriter.writeArrayHeader(out, 0);
                } else {
                    Set<String> set = obj.getSet();
                    EvictionManager.touchKey(key);
                    RespWriter.writeArray(out, set);
                }
                break;
            }

            case "SISMEMBER": {
                if (commandTokens.size() < 3) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'SISMEMBER'");
                    break;
                }
                String key = commandTokens.get(1);
                String member = commandTokens.get(2);
                RedisObject obj = dataStore.get(key);
                if (obj != null && obj.getType() != DataType.SET) {
                    RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    break;
                }

                if (obj == null || isExpired(key)) {
                    RespWriter.writeInteger(out, 0);
                } else {
                    Set<String> set = obj.getSet();
                    EvictionManager.touchKey(key);
                    RespWriter.writeInteger(out, (set.contains(member)) ? 1 : 0);
                }
                break;
            }
            case "SREM": {
                if (commandTokens.size() < 3) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'SREM'");
                    break;
                }
                String key = commandTokens.get(1);
                RedisObject obj = dataStore.get(key);
                if (obj != null && obj.getType() != DataType.SET) {
                    RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    break;
                }

                if (obj == null || isExpired(key)) {
                    RespWriter.writeInteger(out, 0);
                } else {
                    Set<String> set = obj.getSet();
                    EvictionManager.touchKey(key);

                    int removedCount = 0;
                    for (int i = 2; i < commandTokens.size(); i++) {
                        if (set.remove(commandTokens.get(i))) {
                            removedCount++;
                        }
                    }

                    if (set.isEmpty()) {
                        dataStore.remove(key);
                        ttlstore.remove(key);
                        EvictionManager.removeKey(key);
                    }
                    RespWriter.writeInteger(out, removedCount);
                }
                break;
            }
            case "EXPIRE":
                if (commandTokens.size() < 3) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'EXPIRE'");
                } else {
                    String key = commandTokens.get(1);
                    try {
                        long seconds = Long.parseLong(commandTokens.get(2));
                        if (isExpired(key) || !dataStore.containsKey(key)) {
                            RespWriter.writeInteger(out, 0); // 0->key does not exist
                        } else {
                            long now = System.currentTimeMillis();
                            long expireAt;
                            try {
                                expireAt = Math.addExact(now, Math.multiplyExact(seconds, 1000));
                            } catch (ArithmeticException e) {
                                RespWriter.writeError(out, "ERR value is not an integer or out of range");
                                break;
                            }
                            ttlstore.put(key, expireAt);
                            if (expireAt <= now) {
                                dataStore.remove(key);
                                ttlstore.remove(key);
                                EvictionManager.removeKey(key);
                            }
                            RespWriter.writeInteger(out, 1); // 1->TTL set successfully
                        }
                    } catch (NumberFormatException e) {
                        RespWriter.writeError(out, "ERR value is not an integer or out of range");
                    }
                }
                break;

            case "TTL":
                if (commandTokens.size() < 2) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'TTL'");
                } else {
                    String key = commandTokens.get(1);
                    if (isExpired(key) || !dataStore.containsKey(key)) {
                        RespWriter.writeInteger(out, -2); // -2->key does not exist
                    } else if (!ttlstore.containsKey(key)) {
                        RespWriter.writeInteger(out, -1); // -1->key exists but has no TTL
                    } else {
                        long remainingSec = (ttlstore.get(key) - System.currentTimeMillis()) / 1000;
                        RespWriter.writeInteger(out, Math.max(0, remainingSec));
                    }
                }
                break;

            case "TYPE":
                if (commandTokens.size() < 2) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'TYPE'");
                } else {
                    String key = commandTokens.get(1);
                    RedisObject obj = dataStore.get(key);
                    if (obj == null || isExpired(key)) {
                        RespWriter.writeSimpleString(out, "none");
                    } else {
                        RespWriter.writeSimpleString(out, obj.getType().name().toLowerCase());
                    }
                }
                break;

            case "DEL":
                if (commandTokens.size() < 2) {
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'SET'");
                } else {
                    int removedCount = 0;
                    for (int i = 1; i < commandTokens.size(); i++) {
                        String key = commandTokens.get(i);
                        ttlstore.remove(key);
                        EvictionManager.removeKey(key);
                        if (dataStore.remove(key) != null) {
                            removedCount++;
                        }
                    }
                    RespWriter.writeInteger(out, removedCount);
                }
                break;
            default:
                RespWriter.writeError(out, "ERR unknown command '" + cmd + "'");
                break;
        }
        return out.toByteArray();
    }

    private boolean isExpired(String key) { // passive cleaner
        Long expireAt = ttlstore.get(key);
        if (expireAt != null && System.currentTimeMillis() >= expireAt) { // if key exists but has no TTL .get returns
                                                                          // null. comparing null with a primitive
                                                                          // number causes java to crash with a
                                                                          // NULLPOINTEREXCEPTION
            dataStore.remove(key); // currenttimemillis returns the current UNIX time in milliseconds
            ttlstore.remove(key);
            EvictionManager.removeKey(key);
            return true;
        }
        return false;
    }

}
