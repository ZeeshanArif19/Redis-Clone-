package com.miniredis;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;

public class RedisServer {
    private static final int PORT=6379;
    //shared thread safe in memory key-value store accesible to all client threads
    private static final ConcurrentHashMap<String,String> dataStore= new ConcurrentHashMap<>(); //key-val store
    private static final ConcurrentHashMap<String,Long> ttlstore= new ConcurrentHashMap<>(); //key->Absoulute expiration timestamp in millis
    public static void main(String[] args) throws IOException{
        // creates a thread pool that create new threads as needed, but will reuse previosly constructed threads when they are available
        startActiveCleaner();
        ExecutorService threadPool=Executors.newCachedThreadPool();
        // Replaces: try (ServerSocket serverSocket = new ServerSocket(PORT))
        try (ServerSocket serverSocket = new ServerSocket(PORT, 50, java.net.InetAddress.getByName("0.0.0.0"))) { //Opens up Port 6379 on your machine, Acts as a front door for clients to connect to your server
            System.out.println("Redis server is running on port "+PORT);
            
            while(true){
                Socket clientSocket=serverSocket.accept(); //pauses execution on this line until a client connects
                //once a client connects accept() return a new Socket object that represents the connection to the client
                threadPool.submit(()->handleClient(clientSocket));//hands off the new client socket to a background worker thread
                //because accept blocks,the main thread needs to immediately go back to listening for new connections
                //each client has its own dedicated handleCliend method running in its own thread, so it can block without affecting the main thread
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
    private static void handleClient(Socket clientSocket){
        try(InputStream in=clientSocket.getInputStream();  //opens the raw byte stream for reading and writing, becuase all network communication is done in bytes
            OutputStream out=clientSocket.getOutputStream()){
                
            while(true){
                List<String> commandTokens= RespParser.parse(in); //use respparser to to read and tokenize incoming cmds
                if(commandTokens==null || commandTokens.isEmpty()) break; //if null disconnect

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
                            String key=commandTokens.get(1);
                            String val=commandTokens.get(2);
                            dataStore.put(key,val);
                            ttlstore.remove(key); //overwriting key cancels previous TTL
                            RespWriter.writeSimpleString(out,"OK");
                        }
                        break;
                    case "GET":
                        if(commandTokens.size()<2){
                            RespWriter.writeError(out,"ERR wrong number of arguments for 'SET'");
                        }
                        else{
                            String key=commandTokens.get(1);
                            if(isExpired(key)){
                                RespWriter.writeBulkString(out, null);
                            }
                            else{
                                String val=dataStore.get(key);
                                RespWriter.writeBulkString(out, val);
                            }
                        }
                        break;

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
                                long remainingSec= (ttlstore.get(key)-System.currentTimeMillis()/1000);
                                RespWriter.writeInteger(out, Math.max(0,remainingSec));
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
                            String removed=dataStore.remove(key);
                            RespWriter.writeInteger(out, removed!=null?1:0);
                        }
                        break;
                    default:
                        RespWriter.writeError(out, "ERR unknown command '"+cmd+"'");
                        break;
                }
            }
             
                
        }
        catch(IOException e){
            System.err.println("Client Disconnected: "+e.getMessage());
        }
        finally{
            try{
                clientSocket.close();
            }
            catch(IOException ignored){}
        }
    }
}