package com.miniredis;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;

public class RedisServer {
    private static final int PORT=6379;
    //shared thread safe in memory key-value store accesible to all client threads
    private static final ConcurrentHashMap<String,String> dataStore= new ConcurrentHashMap<>();
    public static void main(String[] args) throws IOException{
        // creates a thread pool that create new threads as needed, but will reuse previosly constructed threads when they are available
        ExecutorService threadPool=Executors.newCachedThreadPool();
        try(ServerSocket serverSocket=new ServerSocket(PORT)){ //Opens up Port 6379 on your machine, Acts as a front door for clients to connect to your server
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
                            dataStore.put(commandTokens.get(1),commandTokens.get(2));
                            RespWriter.writeSimpleString(out,"OK");
                        }
                        break;
                    case "GET":
                        if(commandTokens.size()<2){
                            RespWriter.writeError(out,"ERR wrong number of arguments for 'SET'");
                        }
                        else{
                            String val=dataStore.get(commandTokens.get(1));
                            RespWriter.writeBulkString(out, val);
                        }
                        break;
                    case "DEL":
                        if(commandTokens.size()<2){
                            RespWriter.writeError(out,"ERR wrong number of arguments for 'SET'");
                        }
                        else{
                            String removed=dataStore.remove(commandTokens.get(1));
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