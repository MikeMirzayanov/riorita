package com.codeforces.riorita;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class RioritaBenchmark {
    private static final Random TEST_RANDOM = new Random(System.currentTimeMillis());
    private static final int PORT = 8100;

    private static String getRandomString(int length) {
        StringBuilder result = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            result.append((char)('a' + TEST_RANDOM.nextInt(26)));
        }

        return result.toString();
    }

    private static byte[] getRandomBytes(int length) {
        byte[] bytes = new byte[length];

        TEST_RANDOM.nextBytes(bytes);

        return bytes;
    }

    private static void validate(long total, long size) {
        long iterations = total / size;

        System.out.println("Validation: doing " + iterations + " iterations for size " + size + ".");

        Set<String> keys = new HashSet<>();
        while (keys.size() < iterations / 2) {
            keys.add(getRandomString(32));
        }

        Map<String, byte[]> cache = new HashMap<>();
        List<String> keysList = new ArrayList<>(keys);

        long start = System.currentTimeMillis();

        int NUM_THREADS = 16;
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        List<Future<?>> futures = new ArrayList<>();
        AtomicLong progressCounter = new AtomicLong(0);
        long iterationsPerThread = iterations / NUM_THREADS;

        // Submit tasks for each thread
        for (int t = 0; t < NUM_THREADS; t++) {
            futures.add(executor.submit(() -> {
                Riorita riorita = new Riorita("localhost", 8100);

                try {
                    for (int i = 0; i < iterationsPerThread; i++) {
                        long currentProgress = progressCounter.incrementAndGet();

                        // Log progress every 10,000 iterations
                        if (currentProgress % 10000 == 0) {
                            System.out.println("Done " + currentProgress + " in " + (System.currentTimeMillis() - start) + " ms.");
                        }

                        String key = keysList.get(TEST_RANDOM.nextInt(keysList.size()));

                        {
                            boolean rioritaHas = riorita.has(key);
                            boolean cacheHas = cache.containsKey(key);
                            if (rioritaHas != cacheHas) {
                                //throw new RuntimeException("Invalid has: key.length=" + key.length() + ", key=" + key + ".");
                                System.out.println("ri: " + rioritaHas + ", ca: " + cacheHas);
                            }
                        }

                        byte[] result = riorita.get(key);
                        byte[] expected = cache.get(key);

                        //noinspection DoubleNegation
                        if ((result != null) != (expected != null)) {
                            throw new RuntimeException("Invalid get (has): (result != null)=" + (result != null)
                                    + ", has=" + (expected != null) + ".");
                        }

                        if (result != null) {
                            if (!Arrays.equals(result, cache.get(key))) {
                                throw new RuntimeException("Invalid get.");
                            }
                        }

                        // Perform extra gets for stress testing
                        for (int j = 0; j < 5; j++) {
                            riorita.get(keysList.get(TEST_RANDOM.nextInt(keysList.size())));
                        }

                        // Generate random data and put it into riorita and cache
                        result = getRandomString((int) size).getBytes();
                        riorita.put(key, result);
                        cache.put(key, result);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // Wait for all tasks to finish
        for (Future<?> future : futures) {
            try {
                future.get();  // This will block until the task is complete
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        // Shutdown the executor
        executor.shutdown();

        System.out.println("Completed in " + (System.currentTimeMillis() - start) + " ms.");
    }

    private static void test(long total, long size) throws IOException {
        Riorita riorita = new Riorita("localhost", PORT);

        long iterations = total / size;

        System.out.println("Testing: doing " + iterations + " iterations for size " + size + ".");

        Set<String> keys = new HashSet<>();
        while (keys.size() < iterations / 2) {
            keys.add(getRandomString(32));
        }

        Set<String> cache = new HashSet<>();
        List<String> keysList = new ArrayList<>(keys);

        long start = System.currentTimeMillis();

        for (int i = 0; i < iterations; i++) {
            if (i % 10000 == 0) System.out.println("Done " + i + " in " + (System.currentTimeMillis() - start) + " ms.");

            String key = keysList.get(TEST_RANDOM.nextInt(keysList.size()));

            boolean has = cache.contains(key);
            if (riorita.has(key) != has) {
                throw new RuntimeException("Invalid has.");
            }

            byte[] result = riorita.get(key);
            //noinspection DoubleNegation
            if ((result != null) != has) {
                throw new RuntimeException("Invalid get (has): (result != null)=" + (result != null) + ", has=" + has + ".");
            }

            if (!has) {
                for (int j = 0; j < 5; j++) {
                    riorita.get(getRandomString(32));
                }

                result = getRandomBytes((int) size);
                riorita.put(key, result);
                cache.add(key);
            }
        }

        System.out.println("Completed in " + (System.currentTimeMillis() - start) + " ms.");
    }

    public static void main(String[] args) throws IOException {
        if (args[0].startsWith("val")) {
            long total = Long.parseLong(args[1]);
            long size = Long.parseLong(args[2]);
            validate(total, size);
        }

        if (args[0].startsWith("test")) {
            long total = Long.parseLong(args[1]);
            long size = Long.parseLong(args[2]);
            test(total, size);
        }
    }
}
