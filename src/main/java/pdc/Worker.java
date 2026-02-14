package pdc;

import java.io.*;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.*;
/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    private String workerId;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private volatile boolean running = true;

    public void joinCluster(String masterHost, int port) {
        // TODO: Implement the cluster join protocol
        try {
            workerId = "worker_" + UUID.randomUUID().toString().substring(0, 8);
            socket = new Socket(masterHost, port);
            out = new DataOutputStream(socket.getOutputStream());
            in = new DataInputStream(socket.getInputStream());

            Message joinMsg = new Message();
            joinMsg.magic = "CSM218";
            joinMsg.version = 1;
            joinMsg.type = "JOIN";
            joinMsg.sender = workerId;
            joinMsg.timestamp = System.currentTimeMillis();
            joinMsg.payload = new byte[0];

            sendMessage(joinMsg);

            Message ackMsg = receiveMessage();
            if (ackMsg != null && "ACK".equals(ackMsg.type)) {
                executor.execute(this::listenForTasks);
                executor.execute(this::sendHeartbeats);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        // TODO: Implement internal task scheduling
         while (running) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
    
}
