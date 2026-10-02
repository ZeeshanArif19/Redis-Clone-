package com.miniredis;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.io.IOException;

public class RespWriter {
    public static void writeSimpleString(OutputStream out, String message) throws IOException {
        out.write(("+" + message + "\r\n").getBytes(StandardCharsets.UTF_8)); // Calling .getBytes() encodes that string
                                                                              // into a raw array of binary bytes
                                                                              // (byte[]) formatted as standard UTF-8
                                                                              // text so it can travel across TCP
                                                                              // hardware.
        out.flush();// Forces the OS to send bytes down the wire immediately
    }

    public static void writeError(OutputStream out, String errorMessage) throws IOException {
        // A null error would emit invalid RESP text such as -null; callers must always
        // receive a real error message.
        String message = errorMessage == null ? "ERR internal server error" : errorMessage;
        out.write(("-" + message + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    public static void writeInteger(OutputStream out, long val) throws IOException {
        out.write((":" + val + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    public static void writeBulkString(OutputStream out, String val) throws IOException {
        if (val == null) {
            out.write("$-1\r\n".getBytes(StandardCharsets.UTF_8));
        } else {
            byte[] bytes = val.getBytes(StandardCharsets.UTF_8);
            out.write(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            out.write(bytes);
            out.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        out.flush();
    }

    // Array support
    // writes null array
    public static void writeNullArray(OutputStream out) throws IOException {
        out.write("*-1\r\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    // writes RESP array header
    public static void writeArrayHeader(OutputStream out, int count) throws IOException {
        out.write(("*" + count + "\r\n").getBytes(StandardCharsets.UTF_8));
    }

    // Serializes a java collection into a RESP array of Bulk Strings
    public static void writeArray(OutputStream out, Collection<String> items) throws IOException {
        if (items == null) {
            writeNullArray(out);
            return;
        }
        writeArrayHeader(out, items.size());
        for (String item : items) {
            writeBulkString(out, item);
        }
    }
}
// 1. The Terminal Executes \r\n Instead of Displaying It
// \r (Carriage Return): Tells the terminal cursor, "Move all the way back to
// the left margin."
// \n (Line Feed): Tells the terminal cursor, "Move down to the next line."
