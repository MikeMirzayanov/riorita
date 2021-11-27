package com.codeforces.riorita.test;

import com.codeforces.riorita.Riorita;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Mike Mirzayanov (mirzayanovmr@gmail.com)
 */
public class Main {
    public static final String HIT_FACTOR_PREFIX = "--hit.factor=";
    private static double HIT_FACTOR = 0.9;

    public static final String RIORITA_HOST_PREFIX = "--riorita.host=";
    private static String RIORITA_HOST = "127.0.0.1";

    public static final String RIORITA_PORT_PREFIX = "--riorita.port=";
    private static int RIORITA_PORT = 8100;

    public static final String REQUEST_COUNT_PREFIX = "--n=";
    private static int REQUEST_COUNT = 10000;

    public static final String CONCURRENCY_PREFIX = "--c=";
    private static int CONCURRENCY = 4;

    public static final String MAX_PAYLOAD_SIZE_PREFIX = "--max.payload=";
    private static int MAX_PAYLOAD_SIZE = 10000000;

    public static final String AVE_PAYLOAD_SIZE_PREFIX = "--ave.payload=";
    private static int AVE_PAYLOAD_SIZE = 100000;

    private static List<String> keys = Collections.synchronizedList(new ArrayList<String>());

    private static AtomicLong totalPayloadSize = new AtomicLong();

    private static ThreadLocal<Random> random = new ThreadLocal<Random>() {
        @Override
        protected Random initialValue() {
            return new Random();
        }
    };

    private static String getKey() {
        int index = random.get().nextInt(keys.size());
        return keys.get(index);
    }

    private static String getRandomKey() {
        return DigestUtils.sha1Hex(random.get().nextLong() + "$" + random.get().nextDouble() + "$" + System.nanoTime())
                + "_"
                + (1 + random.get().nextInt(MAX_PAYLOAD_SIZE));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length == 1 && args[0].equals("--help")) {
            System.out.println("Usage: jaava -cp riorita-test-jar-with-dependencies.jar Main [--hit.factor=0..1] [--riorita.host=host] [--riorita.port=port]");
        }

        for (String arg : args) {
            if (arg.startsWith(HIT_FACTOR_PREFIX)) {
                HIT_FACTOR = Double.parseDouble(arg.substring(HIT_FACTOR_PREFIX.length()));
            }
            if (arg.startsWith(RIORITA_HOST_PREFIX)) {
                RIORITA_HOST = arg.substring(RIORITA_HOST_PREFIX.length());
            }
            if (arg.startsWith(RIORITA_PORT_PREFIX)) {
                RIORITA_PORT = Integer.valueOf(arg.substring(RIORITA_PORT_PREFIX.length()));
            }
            if (arg.startsWith(REQUEST_COUNT_PREFIX)) {
                REQUEST_COUNT = Integer.valueOf(arg.substring(REQUEST_COUNT_PREFIX.length()));
            }
            if (arg.startsWith(REQUEST_COUNT_PREFIX)) {
                REQUEST_COUNT = Integer.valueOf(arg.substring(REQUEST_COUNT_PREFIX.length()));
            }
            if (arg.startsWith(CONCURRENCY_PREFIX)) {
                CONCURRENCY = Integer.valueOf(arg.substring(CONCURRENCY_PREFIX.length()));
            }
            if (arg.startsWith(MAX_PAYLOAD_SIZE_PREFIX)) {
                MAX_PAYLOAD_SIZE = Integer.valueOf(arg.substring(MAX_PAYLOAD_SIZE_PREFIX.length()));
            }
            if (arg.startsWith(AVE_PAYLOAD_SIZE_PREFIX)) {
                AVE_PAYLOAD_SIZE = Integer.valueOf(arg.substring(AVE_PAYLOAD_SIZE_PREFIX.length()));
            }
        }

        System.out.println("Uses:");
        System.out.println("    " + HIT_FACTOR_PREFIX + HIT_FACTOR);
        System.out.println("    " + RIORITA_HOST_PREFIX + RIORITA_HOST);
        System.out.println("    " + RIORITA_PORT_PREFIX + RIORITA_PORT);
        System.out.println("    " + REQUEST_COUNT_PREFIX + REQUEST_COUNT);
        System.out.println("    " + CONCURRENCY_PREFIX + CONCURRENCY);
        System.out.println("    " + MAX_PAYLOAD_SIZE_PREFIX + MAX_PAYLOAD_SIZE);
        System.out.println("    " + AVE_PAYLOAD_SIZE_PREFIX + AVE_PAYLOAD_SIZE);

        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENCY);

        for (int i = 0; i < REQUEST_COUNT; i++) {
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Riorita riorita = RioritaHolder.riorita.get();
                        if (getRandomDouble() < 0.9) {
                            boolean hit = !keys.isEmpty() && getRandomDouble() < HIT_FACTOR;

                            if (hit) {
                                String key = getKey();
                                String[] sha1AndLength = key.split("_");

                                byte[] bytes = riorita.get(key);

                                if (bytes.length != Integer.valueOf(sha1AndLength[1])) {
                                    throw new RuntimeException("Unexpected length.");
                                }

                                if (!DigestUtils.sha1Hex(bytes).equals(sha1AndLength[0])) {
                                    throw new RuntimeException("Unexpected sha1.");
                                }
                            } else {
                                String key = getRandomKey();
                                if (riorita.get(key) != null) {
                                    throw new RuntimeException("Expected null.");
                                }
                            }
                        } else {
                            int size = getPayloadSize();
                            byte[] bytes = new byte[size];
                            random.get().nextBytes(bytes);
                            String key = DigestUtils.sha1Hex(bytes) + "_" + bytes.length;
                            if (!riorita.put(key, bytes)) {
                                throw new RuntimeException("Expected true.");
                            }
                            totalPayloadSize.addAndGet(bytes.length);
                        }
                    } catch (Exception e) {
                        System.out.println("Unexpected error: " + e.getMessage());
                        System.out.flush();
                        System.exit(1);
                    }
                }
            });
        }

        long startTimeMillis = System.currentTimeMillis();

        pool.shutdown();
        pool.awaitTermination(1L, TimeUnit.DAYS);

        System.out.println("Done in " + (System.currentTimeMillis() - startTimeMillis) + " ms [written " + totalPayloadSize + " bytes].");
    }

    private static int getPayloadSize() {
        double r = random.get().nextDouble();
        r = r * r;

        if (r < 0.25) {
            return (int) Math.round(AVE_PAYLOAD_SIZE * r / 0.25);
        } else {
            return (int) Math.round(AVE_PAYLOAD_SIZE + (MAX_PAYLOAD_SIZE - AVE_PAYLOAD_SIZE) * (r - 0.25) / 0.75);
        }
    }

    private static double getRandomDouble() {
        return random.get().nextDouble();
    }

    private static class RioritaHolder {
        private static ThreadLocal<Riorita> riorita = new ThreadLocal<Riorita>() {
            @Override
            protected Riorita initialValue() {
                return new Riorita(RIORITA_HOST, RIORITA_PORT);
            }
        };
    }
}
