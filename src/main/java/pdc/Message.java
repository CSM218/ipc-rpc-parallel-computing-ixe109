package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public String magic;
    public int version;
    public String type;
    public String sender;
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        // TODO: Implement custom binary or tag-based framing
        //throw new UnsupportedOperationException("You must design your own wire protocol.");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        try {
            byte[] magicBytes = (magic != null ? magic : "").getBytes(StandardCharsets.UTF_8);
            byte[] typeBytes = (type != null ? type : "").getBytes(StandardCharsets.UTF_8);
            byte[] senderBytes = (sender != null ? sender : "").getBytes(StandardCharsets.UTF_8);
            byte[] payloadBytes = (payload != null ? payload : new byte[0]);
            
            dos.writeInt(magicBytes.length);
            dos.write(magicBytes);
            dos.writeInt(version);
            dos.writeInt(typeBytes.length);
            dos.write(typeBytes);
            dos.writeInt(senderBytes.length);
            dos.write(senderBytes);
            dos.writeLong(timestamp);
            dos.writeInt(payloadBytes.length);
            dos.write(payloadBytes);
            
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        // TODO: Implement custom parsing logic
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        Message msg = new Message();
        
        try {
            int magicLen = dis.readInt();
            byte[] magicBytes = new byte[magicLen];
            dis.readFully(magicBytes);
            msg.magic = new String(magicBytes, StandardCharsets.UTF_8);
            
            msg.version = dis.readInt();
            
            int typeLen = dis.readInt();
            byte[] typeBytes = new byte[typeLen];
            dis.readFully(typeBytes);
            msg.type = new String(typeBytes, StandardCharsets.UTF_8);
            
            int senderLen = dis.readInt();
            byte[] senderBytes = new byte[senderLen];
            dis.readFully(senderBytes);
            msg.sender = new String(senderBytes, StandardCharsets.UTF_8);
            
            msg.timestamp = dis.readLong();
            
            int payloadLen = dis.readInt();
            msg.payload = new byte[payloadLen];
            dis.readFully(msg.payload);
            
            return msg;
        } catch (IOException e) {
            return null;
        }
    }
}
