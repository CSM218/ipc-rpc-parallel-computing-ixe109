package pdc;

import java.io.*;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.*;

public class Worker {

    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    private String workerId;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private volatile boolean running = false;

    public void joinCluster(String masterHost, int port) {
        try {
            workerId = "worker_" + UUID.randomUUID().toString().substring(0, 8);
            socket = new Socket(masterHost, port);
            socket.setSoTimeout(5000);
            out = new DataOutputStream(socket.getOutputStream());
            in = new DataInputStream(socket.getInputStream());

            Message joinMsg = new Message();
            joinMsg.magic = "CSM218";
            joinMsg.version = 1;
            joinMsg.type = "JOIN";
            joinMsg.sender = workerId;
            joinMsg.timestamp = System.currentTimeMillis();
            joinMsg.messageType = "HANDSHAKE_REQUEST";

            ByteArrayOutputStream capBaos = new ByteArrayOutputStream();
            DataOutputStream capDos = new DataOutputStream(capBaos);
            capDos.writeUTF("CAPABILITY:COMPUTE");
            capDos.writeInt(Runtime.getRuntime().availableProcessors());
            capDos.flush();
            joinMsg.payload = capBaos.toByteArray();

            sendMessage(joinMsg);

            Message ackMsg = receiveMessage();
            if (ackMsg != null && "ACK".equals(ackMsg.type)) {
                running = true;
                executor.execute(this::listenForTasks);
                executor.execute(this::sendHeartbeats);
            }

        } catch (IOException e) {
            running = false;
        }
    }

    public void execute() {
        if (!running || socket == null || !socket.isConnected()) {
            return;
        }

        executor.execute(() -> {
            while (running) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
    }

    private void listenForTasks() {
        while (running) {
            try {
                Message msg = receiveMessage();
                if (msg == null) break;

                if ("TASK".equals(msg.type)) {
                    executor.execute(() -> processTask(msg));
                }
            } catch (IOException e) {
                running = false;
                break;
            }
        }
    }

    private void processTask(Message taskMsg) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(taskMsg.payload);
            DataInputStream dis = new DataInputStream(bais);

            int tidLen = dis.readInt();
            byte[] tidBytes = new byte[tidLen];
            dis.readFully(tidBytes);
            String taskId = new String(tidBytes);

            int opLen = dis.readInt();
            byte[] opBytes = new byte[opLen];
            dis.readFully(opBytes);
            String operation = new String(opBytes);

            int startRow = dis.readInt();
            int endRow = dis.readInt();
            int cols = dis.readInt();

            int[][] matrix = new int[endRow - startRow][cols];
            for (int r = 0; r < endRow - startRow; r++) {
                for (int c = 0; c < cols; c++) {
                    matrix[r][c] = dis.readInt();
                }
            }

            int[][] result = performOperation(operation, matrix);

            ByteArrayOutputStream resultBaos = new ByteArrayOutputStream();
            DataOutputStream resultDos = new DataOutputStream(resultBaos);
            resultDos.writeInt(startRow);
            resultDos.writeInt(endRow);
            for (int r = 0; r < result.length; r++) {
                for (int c = 0; c < result[r].length; c++) {
                    resultDos.writeInt(result[r][c]);
                }
            }
            resultDos.flush();

            ByteArrayOutputStream finalBaos = new ByteArrayOutputStream();
            finalBaos.write(taskId.getBytes());
            finalBaos.write('|');
            finalBaos.write(resultBaos.toByteArray());

            Message resultMsg = new Message();
            resultMsg.magic = "CSM218";
            resultMsg.version = 1;
            resultMsg.type = "RESULT";
            resultMsg.sender = workerId;
            resultMsg.timestamp = System.currentTimeMillis();
            resultMsg.payload = finalBaos.toByteArray();

            synchronized (this) {
                sendMessage(resultMsg);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int[][] performOperation(String operation, int[][] matrix) {
        if ("BLOCK_MULTIPLY".equals(operation)) {
            int[][] result = new int[matrix.length][matrix[0].length];
            for (int i = 0; i < matrix.length; i++) {
                for (int j = 0; j < matrix[i].length; j++) {
                    result[i][j] = matrix[i][j] * 2;
                }
            }
            return result;
        } else if ("TRANSPOSE".equals(operation)) {
            int[][] result = new int[matrix[0].length][matrix.length];
            for (int i = 0; i < matrix.length; i++) {
                for (int j = 0; j < matrix[i].length; j++) {
                    result[j][i] = matrix[i][j];
                }
            }
            return result;
        } else {
            return matrix;
        }
    }

    private void sendHeartbeats() {
        while (running) {
            try {
                Thread.sleep(800);
                Message hb = new Message();
                hb.magic = "CSM218";
                hb.version = 1;
                hb.type = "HEARTBEAT";
                hb.sender = workerId;
                hb.timestamp = System.currentTimeMillis();
                hb.payload = new byte[0];

                synchronized (this) {
                    if (out != null && socket != null && socket.isConnected()) {
                        sendMessage(hb);
                    }
                }
            } catch (Exception e) {
                running = false;
                break;
            }
        }
    }

    private synchronized void sendMessage(Message msg) throws IOException {
        byte[] data = msg.pack();
        out.writeInt(data.length);
        out.write(data);
        out.flush();
    }

    private Message receiveMessage() throws IOException {
        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data);
        return Message.unpack(data);
    }

    public void shutdown() {
        running = false;
        try {
            if (socket != null) socket.close();
        } catch (IOException ignored) {}
        executor.shutdownNow();
    }
}