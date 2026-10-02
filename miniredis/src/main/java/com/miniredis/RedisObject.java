package com.miniredis;

import java.util.Deque;
import java.util.Map;
import java.util.Set;

public class RedisObject {
    public enum DataType {
        STRING, HASH, LIST, SET
    }

    private final DataType type;
    private final Object value;

    public RedisObject(DataType type, Object value) {
        this.type = type;
        this.value = value;
    }

    public DataType getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    public String getString() {
        return (String) value;
    }

    // The unchecked casts live here so command code can use typed values without
    // repeating them.
    @SuppressWarnings("unchecked")
    public Map<String, String> getHash() {
        return (Map<String, String>) value;
    }

    @SuppressWarnings("unchecked")
    public Deque<String> getList() {
        return (Deque<String>) value;
    }

    @SuppressWarnings("unchecked")
    public Set<String> getSet() {
        return (Set<String>) value;
    }
}
