package com.miniredis;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.miniredis.RedisObject.DataType;

import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Deque;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;

public class RedisServer {
    private static final int PORT=6379;
    //shared thread safe in memory key-value store accesible to all client threads
    private static final ConcurrentHashMap<String,RedisObject> dataStore= new ConcurrentHashMap<>(); //key-val store
    private static final ConcurrentHashMap<String,Long> ttlstore= new ConcurrentHashMap<>(); //key->Absoulute expiration timestamp in millis

    public static void main(String[] args) throws IOException{
        startActiveCleaner();
        //Step1 setting up the non blocking server
        Selector selector=Selector.open();
        ServerSocketChannel serverChannel=ServerSocketChannel.open();
        //Step2
        serverChannel.bind(new InetSocketAddress("0.0.0.0",PORT));
        serverChannel.configureBlocking(false); //turns off standard java thread-blocking behavior
        //Step 3
        serverChannel.register(selector,SelectionKey.OP_ACCEPT); //tells the Selector - "Alert me whenever a new client tries to connect to port 6379"
        System.out.println("Redis server runing on port "+PORT+" Non Blocking Event Loop...");
        
        //Step4 Single threaded event loop
        while(true){
            selector.select(); //Sleep until at least one event occurs
            // The main thread pauses here until the OS kernel signals that a socket has data ready
            Set<SelectionKey> selectedKeys=selector.selectedKeys(); // gives us a list of triggered alerts
            Iterator<SelectionKey> iter=selectedKeys.iterator();

            while(iter.hasNext()){
                SelectionKey key=iter.next();
                iter.remove(); //If we dont remove the key after reading it, java leaves it in the list and attempts to process it again 

                if(!key.isValid()) continue; //gaurds against stale or cancelled keys, if a client disconnects or a socket closes mid loop its SelectionKey is cancelled

                if(key.isAcceptable()){ //Triggers when a new client tries to establish a TCP connection on port
                    handleAccept(serverChannel,selector);//accept the connection and register the new socket with the Selector
                }
                else if(key.isReadable()){ //triggers when an existing client sends bytes across the TCP connection
                    handleRead(key);
                }
            }
        }
    }
    //Accept new TCP connection in non-blocking mode
    private static void handleAccept(ServerSocketChannel serverChannel, Selector selector) throws IOException{
        SocketChannel clientChannel=serverChannel.accept(); //if a client is there it gives you a SocketChannel, if not returns null without stalling execution
        if(clientChannel!=null){
            clientChannel.configureBlocking(false);
            clientChannel.register(selector,SelectionKey.OP_READ,new ByteArrayOutputStream());
            //Each connected client recieves its own dedicated ByteArrayOutputStream attached to its key.This acts as a per-client scratchpad for accumulating TCP data chunks
        }
    }
    //Reading data and handling packet fragmentation
    //TCP transfers data across network boundaries in chunks
    private static void handleRead(SelectionKey key){
        SocketChannel clientChannel=(SocketChannel) key.channel();
        ByteArrayOutputStream clientBuffer=(ByteArrayOutputStream) key.attachment();
        ByteBuffer readBuffer=ByteBuffer.allocate(1024);
        
        try{
            int bytesRead=clientChannel.read(readBuffer); //Grabs whatever bytes are sitting in the network card's hardware queue right now and return immediately
            if(bytesRead==-1){
                clientChannel.close();
                return;
            }
            clientBuffer.write(readBuffer.array(),0,bytesRead); //append those bytes to clientBuffer, if the command is complete RespParser parses it, 
            // if only half then RespParser throws a temporary catchable frame error and the server leaves the partial bytes in clientBuffer to wait for the next OP_READ event
            processClientBuffer(clientChannel,clientBuffer);
        }
        catch(IOException e){
            try{ clientChannel.close();} catch(IOException ignored){}
        }
    }
    //handles message framing and buffer management
    private static void processClientBuffer(SocketChannel clientChannel, ByteArrayOutputStream clientBuffer) throws IOException {
        byte[] rawData = clientBuffer.toByteArray();
        // Inline commands(PING,SET) must end with Enter.
        // RESP commands start with '*' and can be parsed immediately when complete.
        if (rawData.length == 0) {
            return;
        }
        boolean respCommand = rawData[0] == '*';
        if (!respCommand && rawData[rawData.length - 1] != '\n') { //if the payload is an inline cmd it must end with a newline(\n). If no newline has arrived yet, the command is incomplete
            return; //the method return immediately to wait for the remaining bytes from the network
        }

        ByteArrayInputStream input = new ByteArrayInputStream(rawData);
        while (input.available() > 0) { //handles cmd pipelining, if a client sends 3 cmds in a single TCO packet this loop parses all three in a single pass
            input.mark(rawData.length); //saves a bookmark at the exaxt position of input to rewind in case of Exception, .reset() uses this mark

            List<String> commandTokens;
            try {
                commandTokens = RespParser.parse(input);
            } catch (Exception e) {
                input.reset(); // if RespParser throws an exception because the packet was cut off, input.reset() rewinds the stream pointer back where the incomplete cmd began,
                //so the server can start parsing again when remaining bytes arrive
                break; // command is incomplete
            }
            if (commandTokens == null || commandTokens.isEmpty()) {
                break;
            }
            executeCommand(clientChannel, commandTokens);

            byte[] remaining = input.readAllBytes(); //extract any leftover bytes
            clientBuffer.reset(); //clears the clientbuffer so that the previous cmd wont be executed twice
            clientBuffer.write(remaining);

            if (remaining.length == 0) {
                break;
            }

            rawData = remaining; //if bytes remain this refreshes input with those bytes, so while can loop around and parse the next cmd in same pass
            input = new ByteArrayInputStream(remaining);

            // if the rem unparsed bytes start an inline cmd
            if (rawData[0] != '*' && rawData[rawData.length - 1] != '\n') {
                break;
            }
        }
    }
    private static boolean isExpired(String key){ //passive cleaner
        Long expireAt=ttlstore.get(key);
        if(expireAt!=null && System.currentTimeMillis()>expireAt){ //if key exists but has no TTL .get returns null. comparing null with a primitive number causes java to crash with a NULLPOINTEREXCEPTION
            dataStore.remove(key); //currenttimemillis returns the current UNIX time in milliseconds
            ttlstore.remove(key);
            return true;
        }
        return false;
    }
    private static void startActiveCleaner(){ //active cleaner
        //Spawns a background worker thread dedicated to executing timed/periodic tasks.
        ScheduledExecutorService cleaner= Executors.newSingleThreadScheduledExecutor(r->{ 
            Thread t= new Thread(r,"redis-ttl-cleaner"); //name the thread explicitly for debugging
            t.setDaemon(true); //Normal Java threads keep the entire JVM process running indefinitely. If this thread were normal, pressing Ctrl+C or stopping your main server would hang the JVM because this background thread would still be running.
            //Setting setDaemon(true) tells the JVM: "This is a background worker. If all main user threads stop, kill this thread automatically and exit."
            return t;
        });

        cleaner.scheduleAtFixedRate(()->{
            long now=System.currentTimeMillis();
            for(Map.Entry<String,Long> entry: ttlstore.entrySet()){
                if(now>entry.getValue()){
                    dataStore.remove(entry.getKey());
                    ttlstore.remove(entry.getKey());
                }
            }
        },100,100,TimeUnit.MILLISECONDS); //task, initialDelay(waits 100ms after the server starts before running sweep), period(repeadt the task every 100ms), Unit of time
    }

    private static void executeCommand(SocketChannel clientChannel,List<String> commandTokens) throws IOException{
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String cmd=commandTokens.get(0).toUpperCase();


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
                if(commandTokens.size()<3){
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
        //we must use a ByteBuffer to write to a SocketChannel
        //ByteArrayOutputStream(java.io)- is a high level helper class that acts as an in-memory, auto-expanding byte array. has no connections to NIO channels
        //ByteBuffer is java's abstraction for low-level memory management, it keeps track of memory pointers, it can use Off-Heap Direct memory(.allocateDirect()) allowing the OS to copy bytes straight to the NC without JVM memory copies
        ByteBuffer responseBuffer = ByteBuffer.wrap(out.toByteArray()); //Translates raw bytes into an NIO compatible buffer
        while (responseBuffer.hasRemaining()) {
            clientChannel.write(responseBuffer); //.write() only accepts ByteBuffer
        }
    }
}