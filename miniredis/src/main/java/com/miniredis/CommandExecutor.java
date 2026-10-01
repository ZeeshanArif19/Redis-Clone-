package com.miniredis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
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

    public byte[] execute(List<String> commandTokens) throws IOException{
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String cmd = commandTokens.get(0).toUpperCase();
        
        switch(cmd){
            case "PING":
                RespWriter.writeSimpleString(out, "PONG");
                break;

            case "SET":
                if(commandTokens.size()<3){
                    RespWriter.writeError(out,"ERR wrong number of arguments for 'SET'");
                }
                else{
                    //enforce eviction before storing new data
                    if(!EvictionManager.evictIfNecessary(dataStore, ttlstore)){
                        RespWriter.writeError(out, "OOM command not allowed when used memory > maxmemory");
                        break;
                    }
                    String key=commandTokens.get(1);
                    String val=commandTokens.get(2);
                    dataStore.put(key,new RedisObject(DataType.STRING,val));
                    ttlstore.remove(key); //overwriting key cancels previous TTL

                    EvictionManager.touchKey(key);
                    RespWriter.writeSimpleString(out,"OK");
                }
                break;

            case "GET":
                if(commandTokens.size()<2){
                    RespWriter.writeError(out,"ERR wrong number of arguments for 'GET'");
                }
                else{
                    String key=commandTokens.get(1);
                    RedisObject obj=dataStore.get(key);

                    if(obj!=null && obj.getType()!=DataType.STRING){
                        RespWriter.writeError(out,"WRONGTYPE Operation against a key holding the wrong kind of value");
                        break;
                    }
                    if(isExpired(key)){
                        RespWriter.writeBulkString(out, null);
                    }
                    else{
                        String val= (obj==null)? null: (String) obj.getValue();
                        if(val!=null){
                            EvictionManager.touchKey(key);
                        }
                        RespWriter.writeBulkString(out, val);
                    }
                }
                break;

            case "HSET" : {
                if(commandTokens.size()<4){
                    RespWriter.writeError(out,"ERR wrong number of arguments for 'HSET'");
                    break;
                }
                else{
                    String key= commandTokens.get(1);
                    String field = commandTokens.get(2);
                    String value=commandTokens.get(3);
                    
                    RedisObject obj=dataStore.get(key);
                    if(obj!=null && obj.getType()!=DataType.HASH){
                        RespWriter.writeError(out,"WRONGTYPE Operation against a key holding the wrong kind of value");
                        break;
                    }

                    Map<String,String> map;
                    if(obj==null){
                        map=new HashMap<>();
                        dataStore.put(key,new RedisObject(DataType.HASH,map));
                    }
                    else{
                        map=(Map<String,String>) obj.getValue();
                    }
                    
                    boolean isNewField=!map.containsKey(field);
                    map.put(field,value);
                    ttlstore.remove(key);
                    RespWriter.writeInteger(out, isNewField?1:0);
                }
                break;
            }
            case "LPUSH": {
                if(commandTokens.size()<3){
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'LPUSH'");
                    break;                
                }
                else{
                    String key=commandTokens.get(1);
                    RedisObject obj= dataStore.get(key);

                    if(obj!=null && obj.getType()!=DataType.LIST){
                        RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                        break;
                    }

                    Deque<String> list;
                    if(obj==null){
                        list=new ArrayDeque<>();
                        dataStore.put(key,new RedisObject(DataType.LIST,list));
                    }
                    else{
                        list=(Deque<String>) obj.getValue();
                    }

                    for(int i=2;i<commandTokens.size();i++){
                        list.addFirst(commandTokens.get(i));
                    }

                    RespWriter.writeInteger(out, list.size());
                }
                break;
            }
            case "SADD": {
                if(commandTokens.size()<3){
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'SADD'");
                    break;
                }
                else{
                    String key=commandTokens.get(1);
                    RedisObject obj= dataStore.get(key);
                    
                    if(obj!=null && obj.getType()!=DataType.SET){
                        RespWriter.writeError(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                        break;
                    }

                    Set<String> set;
                    if(obj==null){
                        set=new HashSet<>();
                        dataStore.put(key,new RedisObject(DataType.SET, set));
                    }
                    else{
                        set=(Set<String>) obj.getValue();
                    }

                    int addedCount=0;
                    for(int i=2;i<commandTokens.size();i++){
                        if(set.add(commandTokens.get(i))){
                            addedCount++;
                        }
                    }
                    RespWriter.writeInteger(out, addedCount);
                }
                break;
            }
            case "EXPIRE":
                if(commandTokens.size()<3){
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'EXPIRE'");
                }
                else{
                    String key=commandTokens.get(1);
                    try{
                        long seconds=Long.parseLong(commandTokens.get(2));
                        if(isExpired(key) || !dataStore.containsKey(key)){
                            RespWriter.writeInteger(out,0); //0->key does not exist
                        }
                        else{
                            long expireAt=System.currentTimeMillis()+(seconds*1000);
                            ttlstore.put(key,expireAt);
                            RespWriter.writeInteger(out, 1); //1->TTL set successfully
                        }
                    } catch(NumberFormatException e){
                        RespWriter.writeError(out, "ERR value is not an integer or out of range");
                    }
                }
                break;
            
            case "TTL":
                if(commandTokens.size()<2){
                    RespWriter.writeError(out, "ERR wrong number of arguments for 'TTL'");
                }
                else{
                    String key=commandTokens.get(1);
                    if(isExpired(key) || !dataStore.containsKey(key)){
                        RespWriter.writeInteger(out,-2); //-2->key does not exist
                    }
                    else if(!ttlstore.containsKey(key)){
                        RespWriter.writeInteger(out, -1); //-1->key exists but has no TTL
                    }
                    else{
                        long remainingSec= (ttlstore.get(key)-System.currentTimeMillis())/1000;
                        RespWriter.writeInteger(out, Math.max(0,remainingSec));
                    }
                }
                break;
            
            case "TYPE":
                if(commandTokens.size()<2){
                    RespWriter.writeError(out,"ERR wrong number of arguments for 'TYPE'");
                }
                else{
                    String key=commandTokens.get(1);
                    RedisObject obj=dataStore.get(key);
                    if(obj==null || isExpired(key)){
                        RespWriter.writeSimpleString(out,"none");
                    } else{
                        RespWriter.writeSimpleString(out, obj.getType().name().toLowerCase());
                    }
                }
                break;

            case "DEL":
                if(commandTokens.size()<2){
                    RespWriter.writeError(out,"ERR wrong number of arguments for 'SET'");
                }
                else{
                    String key=commandTokens.get(1);
                    ttlstore.remove(key);
                    RedisObject removed=dataStore.remove(key);
                    RespWriter.writeInteger(out, removed!=null?1:0);
                }
                break;
            default:
                RespWriter.writeError(out, "ERR unknown command '"+cmd+"'");
                break;
        }
        return out.toByteArray();
    }
    private boolean isExpired(String key){ //passive cleaner
        Long expireAt=ttlstore.get(key);
        if(expireAt!=null && System.currentTimeMillis()>expireAt){ //if key exists but has no TTL .get returns null. comparing null with a primitive number causes java to crash with a NULLPOINTEREXCEPTION
            dataStore.remove(key); //currenttimemillis returns the current UNIX time in milliseconds
            ttlstore.remove(key);
            return true;
        }
        return false;
    }
}
