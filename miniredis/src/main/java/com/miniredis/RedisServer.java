package com.miniredis;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisServer {
    private static final int PORT=6379;
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
                
                byte[] buffer=new byte[1024];
                int bytesRead;
                while((bytesRead=in.read(buffer))!=-1){ //reads incoming data into the buffer
                    out.write("PONG\r\n".getBytes()); //PONG is the standard response to a PING in redis
                    out.flush(); //flushes the output stream to ensure that the data is sent immediately, rather than being buffered
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