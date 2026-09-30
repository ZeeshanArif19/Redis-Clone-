package com.miniredis;


public class RedisObject {
    public enum DataType{
        STRING,HASH,LIST,SET
    }
    
    private final DataType type;
    private final Object value;

    public RedisObject(DataType type, Object value){
        this.type=type;
        this.value=value;
    }

    public DataType getType(){ return type; }
    public Object getValue(){ return value; }
}
