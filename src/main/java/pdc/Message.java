package pdc;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Message {
    public static final String PROTOCOL_MAGIC = "CSM218";
    public String magic;
    public int version;
    public String type;
    public String sender;
    public long timestamp;
    public byte[] payload;
    public String messageType;
    public String studentId;

    public Message() {
        this.studentId = System.getenv().getOrDefault("STUDENT_ID", "default_student");
        this.magic = PROTOCOL_MAGIC;
    }

    public boolean validate() {
        if (magic == null || !magic.equals(PROTOCOL_MAGIC)) {
            return false;
        }
        if (version < 1) {
            return false;
        }
        if (type == null || type.isEmpty()) {
            return false;
        }
        return true;
    }

    public byte[] pack() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        try {
            byte[] magicBytes = (magic != null ? magic : PROTOCOL_MAGIC).getBytes(StandardCharsets.UTF_8);
            byte[] typeBytes = (type != null ? type : "").getBytes(StandardCharsets.UTF_8);
            byte[] senderBytes = (sender != null ? sender : "").getBytes(StandardCharsets.UTF_8);
            byte[] payloadBytes = (payload != null ? payload : new byte[0]);
            byte[] messageTypeBytes = (messageType != null ? messageType : "").getBytes(StandardCharsets.UTF_8);
            byte[] studentIdBytes = (studentId != null ? studentId : "").getBytes(StandardCharsets.UTF_8);

            dos.writeInt(magicBytes.length);
            dos.write(magicBytes);
            dos.writeInt(version);
            dos.writeInt(typeBytes.length);
            dos.write(typeBytes);
            dos.writeInt(senderBytes.length);
            dos.write(senderBytes);
            dos.writeLong(timestamp);
            dos.writeInt(messageTypeBytes.length);
            dos.write(messageTypeBytes);
            dos.writeInt(studentIdBytes.length);
            dos.write(studentIdBytes);
            dos.writeInt(payloadBytes.length);

            int totalWritten = 0;
            while (totalWritten < payloadBytes.length) {
                int toWrite = Math.min(8192, payloadBytes.length - totalWritten);
                dos.write(payloadBytes, totalWritten, toWrite);
                totalWritten += toWrite;
            }

            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Message unpack(byte[] data) {
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

            int messageTypeLen = dis.readInt();
            byte[] messageTypeBytes = new byte[messageTypeLen];
            dis.readFully(messageTypeBytes);
            msg.messageType = new String(messageTypeBytes, StandardCharsets.UTF_8);

            int studentIdLen = dis.readInt();
            byte[] studentIdBytes = new byte[studentIdLen];
            dis.readFully(studentIdBytes);
            msg.studentId = new String(studentIdBytes, StandardCharsets.UTF_8);

            int payloadLen = dis.readInt();
            msg.payload = new byte[payloadLen];

            int totalRead = 0;
            while (totalRead < payloadLen) {
                int bytesRead = dis.read(msg.payload, totalRead, payloadLen - totalRead);
                if (bytesRead == -1) break;
                totalRead += bytesRead;
            }

            return msg;
        } catch (IOException e) {
            return null;
        }
    }
}