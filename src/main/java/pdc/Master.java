package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;

public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private ServerSocket serverSocket;
    private final ConcurrentHashMap<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TaskInfo> tasks = new ConcurrentHashMap<>();
    private final BlockingQueue<TaskInfo> pendingTasks = new LinkedBlockingQueue<>();
    private volatile boolean running = true;

    static class WorkerConnection {
        Socket socket;
        DataOutputStream out;
        DataInputStream in;
        String workerId;
        long lastHeartbeat;
        boolean active;

        WorkerConnection(Socket socket, String workerId) throws IOException {
            this.socket = socket;
            this.out = new DataOutputStream(socket.getOutputStream());
            this.in = new DataInputStream(socket.getInputStream());
            this.workerId = workerId;
            this.lastHeartbeat = System.currentTimeMillis();
            this.active = true;
        }

        synchronized void sendMessage(Message msg) throws IOException {
            byte[] data = msg.pack();
            out.writeInt(data.length);
            out.write(data);
            out.flush();
        }

        Message receiveMessage() throws IOException {
            int length = in.readInt();
            byte[] data = new byte[length];
            in.readFully(data);
            return Message.unpack(data);
        }
    }

    static class TaskInfo {
        String taskId;
        String operation;
        byte[] data;
        String assignedWorker;
        long assignedTime;
        boolean completed;
        byte[] result;

        TaskInfo(String taskId, String operation, byte[] data) {
            this.taskId = taskId;
            this.operation = operation;
            this.data = data;
            this.completed = false;
        }
    }

    public Object coordinate(String operation, int[][] data, int workerCount) {
        try {
            listen(5000 + (int)(Math.random() * 1000));
            
            long deadline = System.currentTimeMillis() + 30000;
            while (workers.size() < workerCount && System.currentTimeMillis() < deadline) {
                Thread.sleep(100);
            }

            if (workers.isEmpty()) {
                return null;
            }

            int rows = data.length;
            int cols = data[0].length;
            int effectiveWorkers = Math.min(workerCount, workers.size());
            int tasksCreated = Math.max(effectiveWorkers * 3, Math.min(rows, 20));
            int rowsPerTask = Math.max(1, rows / tasksCreated);

            for (int i = 0; i < tasksCreated; i++) {
                int startRow = i * rowsPerTask;
                int endRow = (i == tasksCreated - 1) ? rows : Math.min((i + 1) * rowsPerTask, rows);
                
                if (startRow >= rows) break;
                
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                dos.writeInt(startRow);
                dos.writeInt(endRow);
                dos.writeInt(cols);
                for (int r = startRow; r < endRow; r++) {
                    for (int c = 0; c < cols; c++) {
                        dos.writeInt(data[r][c]);
                    }
                }
                dos.flush();

                TaskInfo task = new TaskInfo("task_" + i, operation, baos.toByteArray());
                tasks.put(task.taskId, task);
                pendingTasks.offer(task);
            }

            for (int i = 0; i < effectiveWorkers; i++) {
                systemThreads.execute(this::distributeTasks);
            }

            systemThreads.execute(() -> {
                while (running) {
                    try {
                        Thread.sleep(1500);
                        reconcileState();
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            });

            long timeout = System.currentTimeMillis() + 60000;
            while (System.currentTimeMillis() < timeout) {
                boolean allDone = tasks.values().stream().allMatch(t -> t.completed);
                if (allDone) break;
                Thread.sleep(50);
            }

            int[][] result;
            if ("TRANSPOSE".equals(operation)) {
                result = new int[cols][rows];
                for (TaskInfo task : tasks.values()) {
                    if (task.result != null) {
                        ByteArrayInputStream bais = new ByteArrayInputStream(task.result);
                        DataInputStream dis = new DataInputStream(bais);
                        int startRow = dis.readInt();
                        int endRow = dis.readInt();
                        int resultCols = dis.available() / ((endRow - startRow) * 4);
                        for (int r = 0; r < endRow - startRow; r++) {
                            for (int c = 0; c < resultCols; c++) {
                                result[c][startRow + r] = dis.readInt();
                            }
                        }
                    }
                }
            } else {
                result = new int[rows][cols];
                for (TaskInfo task : tasks.values()) {
                    if (task.result != null) {
                        ByteArrayInputStream bais = new ByteArrayInputStream(task.result);
                        DataInputStream dis = new DataInputStream(bais);
                        int startRow = dis.readInt();
                        int endRow = dis.readInt();
                        for (int r = startRow; r < endRow; r++) {
                            for (int c = 0; c < cols; c++) {
                                result[r][c] = dis.readInt();
                            }
                        }
                    }
                }
            }

            shutdown();
            return result;

        } catch (Exception e) {
            return null;
        }
    }

    public void listen(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        systemThreads.execute(() -> {
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    systemThreads.execute(() -> handleWorker(clientSocket));
                } catch (IOException e) {
                    if (running) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    private void handleWorker(Socket socket) {
        try {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            int length = in.readInt();
            byte[] data = new byte[length];
            in.readFully(data);
            Message joinMsg = Message.unpack(data);

            if ("JOIN".equals(joinMsg.type)) {
                String workerId = joinMsg.sender;
                WorkerConnection conn = new WorkerConnection(socket, workerId);
                workers.put(workerId, conn);

                Message ack = new Message();
                ack.magic = "CSM218";
                ack.version = 1;
                ack.type = "ACK";
                ack.sender = "master";
                ack.timestamp = System.currentTimeMillis();
                ack.payload = new byte[0];
                conn.sendMessage(ack);

                systemThreads.execute(() -> workerListener(conn));
            }
        } catch (IOException e) {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private void workerListener(WorkerConnection conn) {
        try {
            while (running && conn.active) {
                Message msg = conn.receiveMessage();
                
                if ("RESULT".equals(msg.type)) {
                    String taskId = new String(msg.payload, 0, msg.payload.length);
                    int separatorIdx = taskId.indexOf('|');
                    if (separatorIdx > 0) {
                        String tid = taskId.substring(0, separatorIdx);
                        byte[] resultData = new byte[msg.payload.length - separatorIdx - 1];
                        System.arraycopy(msg.payload, separatorIdx + 1, resultData, 0, resultData.length);
                        
                        TaskInfo task = tasks.get(tid);
                        if (task != null) {
                            task.result = resultData;
                            task.completed = true;
                        }
                    }
                } else if ("HEARTBEAT".equals(msg.type)) {
                    conn.lastHeartbeat = System.currentTimeMillis();
                }
            }
        } catch (IOException e) {
            conn.active = false;
            workers.remove(conn.workerId);
        }
    }

    private void distributeTasks() {
        while (running) {
            try {
                TaskInfo task = pendingTasks.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    if (tasks.values().stream().allMatch(t -> t.completed || t.assignedWorker != null)) {
                        break;
                    }
                    continue;
                }

                WorkerConnection worker = selectWorker();
                if (worker != null) {
                    Message taskMsg = new Message();
                    taskMsg.magic = "CSM218";
                    taskMsg.version = 1;
                    taskMsg.type = "TASK";
                    taskMsg.sender = "master";
                    taskMsg.timestamp = System.currentTimeMillis();
                    
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(baos);
                    byte[] tidBytes = task.taskId.getBytes();
                    dos.writeInt(tidBytes.length);
                    dos.write(tidBytes);
                    byte[] opBytes = task.operation.getBytes();
                    dos.writeInt(opBytes.length);
                    dos.write(opBytes);
                    dos.write(task.data);
                    dos.flush();
                    taskMsg.payload = baos.toByteArray();

                    try {
                        worker.sendMessage(taskMsg);
                        task.assignedWorker = worker.workerId;
                        task.assignedTime = System.currentTimeMillis();
                    } catch (IOException e) {
                        worker.active = false;
                        pendingTasks.offer(task);
                    }
                } else {
                    pendingTasks.offer(task);
                    Thread.sleep(50);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private int lastWorkerIndex = 0;
    private synchronized WorkerConnection selectWorker() {
        if (workers.isEmpty()) return null;
        
        WorkerConnection[] activeWorkers = workers.values().stream()
            .filter(w -> w.active)
            .toArray(WorkerConnection[]::new);
        
        if (activeWorkers.length == 0) return null;
        
        lastWorkerIndex = (lastWorkerIndex + 1) % activeWorkers.length;
        return activeWorkers[lastWorkerIndex];
    }

    public void reconcileState() {
        long now = System.currentTimeMillis();
        for (WorkerConnection worker : workers.values()) {
            if (worker.active && (now - worker.lastHeartbeat > 5000)) {
                worker.active = false;
                workers.remove(worker.workerId);
                
                for (TaskInfo task : tasks.values()) {
                    if (!task.completed && worker.workerId.equals(task.assignedWorker)) {
                        task.assignedWorker = null;
                        task.assignedTime = 0;
                        pendingTasks.offer(task);
                    }
                }
                
                try {
                    worker.socket.close();
                } catch (IOException ignored) {}
            }
        }
        
        for (TaskInfo task : tasks.values()) {
            if (!task.completed && task.assignedWorker != null && task.assignedTime > 0) {
                long elapsed = now - task.assignedTime;
                if (elapsed > 10000) {
                    WorkerConnection worker = workers.get(task.assignedWorker);
                    if (worker == null || !worker.active) {
                        task.assignedWorker = null;
                        task.assignedTime = 0;
                        pendingTasks.offer(task);
                    }
                }
            }
        }
    }

    private void shutdown() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}
        
        for (WorkerConnection worker : workers.values()) {
            try {
                worker.socket.close();
            } catch (IOException ignored) {}
        }
        systemThreads.shutdownNow();
    }
}