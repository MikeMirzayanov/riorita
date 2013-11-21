package com.codeforces.riorita;

import java.io.IOException;
import java.util.*;

public class RioritaBenchmark {
    private static final Random TEST_RANDOM = new Random(System.currentTimeMillis());

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

    private static void validate(Riorita riorita, long total, long size) throws IOException {
        long iterations = total / size;

        System.out.println("Validation: doing " + iterations + " iterations for size " + size + ".");

        Set<String> keys = new HashSet<>();
        while (keys.size() < iterations / 2) {
            keys.add(getRandomString(32));
        }

        Map<String, byte[]> cache = new HashMap<>();
        List<String> keysList = new ArrayList<>(keys);

        long start = System.currentTimeMillis();

        for (int i = 0; i < iterations; i++) {
            if (i % 10000 == 0) System.out.println("Done " + i + " in " + (System.currentTimeMillis() - start) + " ms.");

            String key = keysList.get(TEST_RANDOM.nextInt(keysList.size()));

            boolean has = cache.containsKey(key);
            if (riorita.has(key) != has) {
                throw new RuntimeException("Invalid has.");
            }

            byte[] result = riorita.get(key);
            //noinspection DoubleNegation
            if ((result != null) != has) {
                throw new RuntimeException("Invalid get (has).");
            }

            if (has) {
                if (!Arrays.equals(result, cache.get(key))) {
                    throw new RuntimeException("Invalid get.");
                }
            }

            for (int j = 0; j < 5; j++) {
                riorita.get(keysList.get(TEST_RANDOM.nextInt(keysList.size())));
            }

            result = getRandomString((int) size).getBytes();
            riorita.put(key, result);
            cache.put(key, result);
        }

        System.out.println("Completed in " + (System.currentTimeMillis() - start) + " ms.");
    }

    private static void test(Riorita riorita, long total, long size) throws IOException {
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
                throw new RuntimeException("Invalid get (has).");
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
        Riorita riorita = new Riorita("localhost", 8024);

        if (args[0].startsWith("val")) {
            long total = Long.parseLong(args[1]);
            long size = Long.parseLong(args[2]);
            validate(riorita, total, size);
        }

        if (args[0].startsWith("test")) {
            long total = Long.parseLong(args[1]);
            long size = Long.parseLong(args[2]);
            test(riorita, total, size);
        }
    }
}
