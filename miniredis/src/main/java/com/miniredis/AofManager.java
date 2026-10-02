package com.miniredis;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class AofManager {
    private static final String AOF_FILE="appendonly.aof";
    private static FileOutputStream aofOut;
    private static boolean isReplaying=false;

    public static void init(){  //opening the writing stream
        try{ //disk ops can fail java forces us to use try catch blocks to handle exceptions gracefully
            File file = new File(AOF_FILE);
            if(!file.exists()){
                file.createNewFile(); //creates an empty appendonly.aof if missing
            }
            //open stream in append mode
            aofOut= new FileOutputStream(file,true);
        } catch(IOException e){
            System.err.println("[AOF] Failed to initialize AOF file: "+e.getMessage());
        }
    }
    //Appending writes to disk
    //synchronized -> ensures that if multiple execution paths trigger disk writes concurrently, whole command frames are written sequentially without corrupting or interleaving the raw bytes
    public static synchronized void logCommand(List<String> commandTokens){
        if(aofOut==null || isReplaying || commandTokens==null || commandTokens.isEmpty()){
            return;
        }
        try{
            StringBuilder resp= new StringBuilder();
            resp.append("*").append(commandTokens.size()).append("\r\n");
            for(String token: commandTokens){
                byte[] bytes=token.getBytes(StandardCharsets.UTF_8); //converts String->UTF-8 encoded bytes for transmission across the disk stream
                resp.append("$").append(bytes.length).append("\r\n");
                resp.append(token).append("\r\n");
            }

            aofOut.write(resp.toString().getBytes(StandardCharsets.UTF_8)); //writes bytes to stream
            aofOut.flush(); //force os to dump RAM to disk
        } catch(IOException e){
            System.err.println("[AOF] Error writing to AOF file: "+e.getMessage());
        }
    }
    //reconstruct data on Startup
    public static void loadAndReplay(CommandExecutor executor){
        File file= new File(AOF_FILE);
        if(!file.exists() || file.length()==0) return;

        System.out.println("[AOF] Loading and replaying commands from " + AOF_FILE + "...");
        isReplaying=true; //gaurd flag to prevent logging commands back into AOF during replay
        //try with resources: Automatically closes file streams when finished
        try(FileInputStream fis= new FileInputStream(file);  //if we dont sclose a file it causes a file descriptor leak
            BufferedInputStream bis= new BufferedInputStream(fis)){
            //Replaying stored commands reconstructs your in-memory dataStore. Passing an empty in-memory dummyStream discards the protocol execution outputs (like "OK" or ":1") instead of trying to write them out to a non-existent socket.
            //ByteArrayOutputStream dummyStream= new ByteArrayOutputStream();
            
            while(bis.available()>0){ //while unread bytes remain in file, as .parse reads data .available decreases
                try{
                    List<String> commandTokens= RespParser.parse(bis); //read and parse RESP frame
                    if(commandTokens!=null && !commandTokens.isEmpty()){
                        executor.execute(commandTokens);
                    }
                } catch(Exception e){
                    break;
                }
            }
            System.out.println("[AOF] Replay complete.");
        } catch(IOException e){
            System.err.println("[AOF] Error during AOF recovery: " + e.getMessage());
        } finally{
            isReplaying=false; //restore normal state
        }
    }
}
