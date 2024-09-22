package com.codeforces.riorita.watchdog;

import com.codeforces.riorita.Riorita;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.*;

public class Watchdog {
    private static final Random RANDOM = new Random(getRandomSeed());
    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private static long getRandomSeed() {
        return Runtime.getRuntime().freeMemory()
                + Runtime.getRuntime().maxMemory()
                + Runtime.getRuntime().totalMemory()
                + System.currentTimeMillis()
                + System.nanoTime()
                + Thread.currentThread().getId()
                + Watchdog.class.hashCode();
    }

    public static void main(String[] args) {
        try {
            if (args.length < 1) {
                System.out.println("Usage: java -jar watchdog.jar <hostPort>"
                        + " [chunkSize=1024] [chunkCount=10] [totalTimoutSec=50]");
                System.exit(1);
            }

            String hostPort = args[0];

            int chunkSize = args.length > 1 ? Integer.parseInt(args[1]) : 1024;
            int chunkCount = args.length > 2 ? Integer.parseInt(args[2]) : 10;
            int totalTimeoutSec = args.length > 3 ? Integer.parseInt(args[3]) : 50;

            System.out.print("Watchdog {hostPort=" + hostPort
                    + ", chunkSize="
                    + chunkSize + ", chunkCount=" + chunkCount
                    + ", totalTimeoutSec=" + totalTimeoutSec + "}: ");

            String[] hostPortItems = hostPort.split(":");
            if (hostPortItems.length < 1 || hostPortItems.length > 2) {
                System.out.println("Invalid hostPort: '" + hostPort + "'");
                System.exit(1);
            }

            String host = hostPortItems[0];
            int port = hostPortItems.length > 1 ? Integer.parseInt(hostPortItems[1]) : 80;

            executeWithTimeout(() -> {
                try {
                    run(host, port, chunkSize, chunkCount, totalTimeoutSec);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    System.exit(1);
                }
                return null;
            }, totalTimeoutSec);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(1);
        } finally {
            executorService.shutdown();
        }
    }

    private static void run(String host, int port,
                            int chunkSize, int chunkCount, int totalTimeoutSec) throws IOException {
        Riorita riorita = new Riorita(host, port);

        long startTimeMillis = System.currentTimeMillis();

        byte[] chunk = new byte[chunkSize];
        for (int i = 0; i < chunkCount; i++) {
            long currentTimeMillis = System.currentTimeMillis();

            if (!riorita.ping()) {
                System.out.println("Failed to ping the server");
                System.exit(1);
            }

            // Check if total timeout has been reached
            if (currentTimeMillis - startTimeMillis > TimeUnit.SECONDS.toMillis(totalTimeoutSec)) {
                System.out.println("Timed out after " + totalTimeoutSec + " sec");
                System.exit(1);
            }

            if (riorita.get(getRandomKey()) != null) {
                System.out.println("Failed to get chunk for non-existing key");
                System.exit(1);
            }

            RANDOM.nextBytes(chunk);
            String randomKey = getRandomKey();
            System.out.println(randomKey);

            // Perform the `put` operation with timeout
            riorita.put(randomKey, chunk);

            // Perform the `get` operation with timeout
            byte[] gotBytes = riorita.get(randomKey);
            if (gotBytes == null || gotBytes.length != chunkSize) {
                System.out.println("Failed to get chunk for key: '" + randomKey + "'");
                System.exit(1);
            }

            // Validate the received chunk
            for (int j = 0; j < chunkSize; j++) {
                if (gotBytes[j] != chunk[j]) {
                    System.out.println("Sent and got chunks are different for key: '"
                            + randomKey + "' [pos=" + j + "]");
                    System.exit(1);
                }
            }

            // Perform the `delete` operation with timeout
            riorita.delete(randomKey);
        }

        long endTimeMillis = System.currentTimeMillis();
        System.out.println("Done in " + (endTimeMillis - startTimeMillis) + " ms");
    }

    @SuppressWarnings("UnusedReturnValue")
    private static <T> T executeWithTimeout(Callable<T> task, int timeoutSec) {
        Future<T> future = executorService.submit(task);
        try {
            return future.get(timeoutSec, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            System.out.println("Operation timed out after " + timeoutSec + " seconds.");
            System.exit(1);
        } catch (InterruptedException | ExecutionException e) {
            System.out.println("Error during operation: " + e.getMessage());
            System.exit(1);
        }
        return null; // This line will never be reached because System.exit will terminate the program
    }

    private static String getRandomKey() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            sb.append((char) ('a' + RANDOM.nextInt(26)));
        }
        return sb.toString();
    }
}
