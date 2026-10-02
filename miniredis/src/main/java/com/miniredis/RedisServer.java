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

import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

public class RedisServer {
    private static final int PORT=6379;
    //shared thread safe in memory key-value store accesible to all client threads
    private static final ConcurrentHashMap<String,RedisObject> dataStore= new ConcurrentHashMap<>(); //key-val store
    private static final ConcurrentHashMap<String,Long> ttlstore= new ConcurrentHashMap<>(); //key->Absoulute expiration timestamp in millis
    private static final CommandExecutor commandExecutor= new CommandExecutor(dataStore, ttlstore);

    public static void main(String[] args) throws IOException{
        CommandExecutor executor = new CommandExecutor(dataStore, ttlstore);
        AofManager.loadAndReplay(executor); //dependency injection -> loadandreplay recieves commandExecutor instance and uses it to reconstruct state
        AofManager.init(); //open aof file for live command appending

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
            
            byte[] response = commandExecutor.execute(commandTokens);
            //we must use a ByteBuffer to write to a SocketChannel
            //ByteArrayOutputStream(java.io)- is a high level helper class that acts as an in-memory, auto-expanding byte array. has no connections to NIO channels
            //ByteBuffer is java's abstraction for low-level memory management, it keeps track of memory pointers, it can use Off-Heap Direct memory(.allocateDirect()) allowing the OS to copy bytes straight to the NC without JVM memory copies
            ByteBuffer responseBuffer = ByteBuffer.wrap(response); //Translates raw bytes into an NIO compatible buffer
            while (responseBuffer.hasRemaining()) {
                clientChannel.write(responseBuffer); //.write() only accepts ByteBuffer
            }

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
}