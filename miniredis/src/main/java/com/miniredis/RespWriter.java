package com.miniredis;
import java.io.OutputStream;
import java.io.IOException;

public class RespWriter {
    public static void writeSimpleString(OutputStream out, String message) throws IOException{
        out.write(("+"+message+"\r\n").getBytes()); //Calling .getBytes() encodes that string into a raw array of binary bytes (byte[]) formatted as standard UTF-8 text so it can travel across TCP hardware.
        out.flush();// Forces the OS to send bytes down the wire immediately
    }
    public static void writeError(OutputStream out,String errorMessage) throws IOException{
        out.write(("-"+errorMessage+"\r\n").getBytes());
        out.flush();
    }
    public static void writeInteger(OutputStream out,long val) throws IOException{
        out.write((":"+val+"\r\n").getBytes());
        out.flush();
    }
    public static void writeBulkString(OutputStream out, String val) throws IOException{
        if(val==null){
            out.write("$-1\r\n".getBytes());
        }
        else{
            byte[] bytes=val.getBytes("UTF-8");
            out.write(("$"+bytes.length+"\r\n"+val+"\r\n").getBytes());
        }
        out.flush();
    }
}
// 1. The Terminal Executes \r\n Instead of Displaying It
// \r (Carriage Return): Tells the terminal cursor, "Move all the way back to the left margin."
// \n (Line Feed): Tells the terminal cursor, "Move down to the next line."
