package com.miniredis;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class RespParser {
    public static List<String> parse(InputStream in) throws IOException{
        int firstByte=in.read(); //reads one byte from socket
        if(firstByte==-1){ //connection closed
            return null;
        }
        char type=(char) firstByte;
        if(type=='*'){
            int arraySize=readInteger(in);
            List<String> commandTokens=new ArrayList<>(arraySize);

            for(int i=0;i<arraySize;i++){
                int nextByte=in.read();
                if(nextByte!='$'){
                    throw new IllegalArgumentException("Expected Bulk String($). got: "+(char) nextByte);
                }
                int length=readInteger(in);
                String bulkString=readBulkString(in,length);
                commandTokens.add(bulkString);
            }
            return commandTokens;
        }
        else{ //inline/plaintext cmds for testing
            String line = type + readLine(in);
            List<String> tokens = new ArrayList<>();
            for (String token : line.trim().split("\\s+")) {
                if (!token.isEmpty()) tokens.add(token);
            }
            return tokens;
        }
    }
    private static int readInteger(InputStream in) throws IOException{ //reads chars until \r\n and parses as integer
        String line=readLine(in);
        return Integer.parseInt(line);
    }
    private static String readBulkString(InputStream in,int length) throws IOException{ //reads eaxct length bytes of data followed by \r\n
        byte[] buffer =new byte[length];
        int totalBytesRead=0;
        while(totalBytesRead<length){ //TCP is a stream oriented protocol not packet oriented when we send 100bytes of data over the wire the client might send those in small chunks due to netwrok delayes or packet fragmentation
            int read=in.read(buffer,totalBytesRead,length-totalBytesRead); //buffer,offset(where in the array to start filling),length
            if(read==-1) throw new IOException("Unexpected end of stream while reading bulk string");
            totalBytesRead+=read;
        }
        in.read(); //consume trailing \r\n
        in.read();
        return new String(buffer,"UTF-8");
    }
    private static String readLine(InputStream in) throws IOException{ //helper to read chars until \r\n delimiter
        StringBuilder sb=new StringBuilder();
        int b;
        while((b=in.read())!=-1){
            if(b=='\r'){
                int next=in.read(); //consume \n
                if(next=='\n') break;
            }
            else{
                sb.append((char) b);
            }
        }
        return sb.toString();
    }
}
