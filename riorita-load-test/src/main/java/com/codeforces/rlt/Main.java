package com.codeforces.rlt;

import com.codeforces.commons.cache.RioritaFsByteCache;

import java.io.File;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class Main {
    private static final String SECTION_PREFIX = "RioritaLoadTest";

    private static final long READ_VALUE_DOMAIN = 0x6A09E667F3BCC909L;
    private static final long WRITE_VALUE_DOMAIN = 0xBB67AE8584CAA73BL;
    private static final long READ_KEY_DOMAIN = 0x3C6EF372FE94F82BL;
    private static final long WRITE_KEY_DOMAIN = 0xA54FF53A5F1D36F1L;
    private static final long MISS_KEY_DOMAIN = 0x1F83D9ABFB41BD6BL;
    private static final long OPERATION_ORDER_DOMAIN = 0x510E527FADE682D1L;
    private static final long READ_CHOICE_DOMAIN = 0x9B05688C2B3E6C1FL;

    private static final long DEFAULT_READ_SET_BYTES = 64L * 1024L * 1024L;
    private static final int MAX_DEFAULT_READ_KEY_COUNT = 4096;

    public static void main(String[] args) throws Exception {
        Config config;
        try {
            config = Config.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            printUsage();
            throw e;
        }

        if (config.help) {
            printUsage();
            return;
        }

        int readKeyCount = config.getReadKeyCount();
        int verifyCount = config.getVerifyCount();
        String section = SECTION_PREFIX + "-" + Long.toHexString(config.seed)
                + "-" + Long.toHexString(System.nanoTime());

        printConfiguration(config, readKeyCount, verifyCount, section);

        RioritaFsByteCache cache = new RioritaFsByteCache(config.directory);
        try {
            runFunctionalChecks(cache, section + "-functional-a", section + "-functional-b");
            System.out.println("Functional checks: OK (miss, empty value, exact bytes, overwrite, isolation, clear).\n");

            ReadSet readSet = prepareReadSet(cache, section, config, readKeyCount);
            WorkloadResult result = runWorkload(cache, section, config, readSet);
            printWorkloadResult(result);

            long validationStart = System.nanoTime();
            validateWrittenValues(cache, section, config, verifyCount);
            assertNull(cache.get(section, key("missing", config.seed, -1, WRITE_KEY_DOMAIN)),
                    "A never-written key unexpectedly exists after the workload");
            double validationMillis = nanosToMillis(System.nanoTime() - validationStart);

            System.out.printf(Locale.ROOT,
                    "Correctness: OK (%d hits checked byte-for-byte, %d misses checked for null, "
                            + "%d/%d writes verified after the run) in %.3f ms.%n",
                    config.getHitCount(), config.getMissCount(), verifyCount, config.writeCount, validationMillis);
        } finally {
            try {
                if (!config.keepData) {
                    cache.clearSection(section);
                    cache.clearSection(section + "-functional-a");
                    cache.clearSection(section + "-functional-b");
                }
            } finally {
                cache.close();
            }
        }
    }

    private static void runFunctionalChecks(RioritaFsByteCache cache, String firstSection, String secondSection) {
        cache.clearSection(firstSection);
        cache.clearSection(secondSection);

        String key = "functional-key";
        assertNull(cache.get(firstSection, key), "A missing key returned a value");

        byte[] empty = new byte[0];
        cache.put(firstSection, key, empty);
        assertBytes(empty, cache.get(firstSection, key), "An empty value was not preserved");

        byte[] source = new byte[]{1, 2, 3, 4, 5, 6, 7};
        byte[] expected = Arrays.copyOf(source, source.length);
        cache.put(firstSection, key, source);
        source[0] = 100;
        assertBytes(expected, cache.get(firstSection, key), "put did not preserve the submitted bytes");

        byte[] returned = cache.get(firstSection, key);
        returned[1] = 101;
        assertBytes(expected, cache.get(firstSection, key), "get exposed mutable cache storage");

        byte[] overwritten = new byte[]{9, 8, 7};
        cache.put(firstSection, key, overwritten);
        assertBytes(overwritten, cache.get(firstSection, key), "An overwrite returned stale bytes");

        byte[] otherSectionValue = new byte[]{11, 12};
        cache.put(secondSection, key, otherSectionValue);
        assertBytes(overwritten, cache.get(firstSection, key), "Sections are not isolated");
        assertBytes(otherSectionValue, cache.get(secondSection, key), "Sections are not isolated");

        cache.clearSection(firstSection);
        assertNull(cache.get(firstSection, key), "clearSection did not clear its section");
        assertBytes(otherSectionValue, cache.get(secondSection, key), "clearSection affected another section");

        cache.clearSection(secondSection);
    }

    private static ReadSet prepareReadSet(
            RioritaFsByteCache cache, String section, Config config, int readKeyCount) {
        String[] keys = new String[readKeyCount];
        byte[][] values = new byte[readKeyCount][];
        long byteCount = 0;
        long start = System.nanoTime();

        for (int i = 0; i < readKeyCount; i++) {
            keys[i] = key("read", config.seed, i, READ_KEY_DOMAIN);
            values[i] = value(config, i, READ_VALUE_DOMAIN);
            byteCount += values[i].length;
            cache.put(section, keys[i], values[i]);
        }

        for (int i = 0; i < readKeyCount; i++) {
            assertBytes(values[i], cache.get(section, keys[i]),
                    "Read-set verification failed for key index " + i);
        }

        long elapsed = System.nanoTime() - start;
        if (readKeyCount > 0) {
            System.out.printf(Locale.ROOT,
                    "Prepared and verified read set: %d keys, %s in %.3f ms.%n%n",
                    readKeyCount, formatBytes(byteCount), nanosToMillis(elapsed));
        }

        return new ReadSet(keys, values);
    }

    private static WorkloadResult runWorkload(
            RioritaFsByteCache cache, String section, Config config, ReadSet readSet) throws Exception {
        final int[] operations = createOperations(config);
        final int hitCount = config.getHitCount();
        final long[] readHitLatencies = new long[hitCount];
        final long[] readMissLatencies = new long[config.getMissCount()];
        final long[] writeLatencies = new long[config.writeCount];
        final long[] threadReadBytes = new long[config.threadCount];
        final long[] threadWriteBytes = new long[config.threadCount];
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final CountDownLatch ready = new CountDownLatch(config.threadCount);
        final CountDownLatch start = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(config.threadCount);
        Future<?>[] futures = new Future<?>[config.threadCount];

        for (int thread = 0; thread < config.threadCount; thread++) {
            final int threadIndex = thread;
            futures[thread] = pool.submit(new Runnable() {
                @Override
                public void run() {
                    long localReadBytes = 0;
                    long localWriteBytes = 0;
                    ready.countDown();
                    try {
                        start.await();

                        for (int operationIndex = threadIndex;
                             operationIndex < operations.length && failure.get() == null;
                             operationIndex += config.threadCount) {
                            int operation = operations[operationIndex];
                            if (operation < 0) {
                                int readIndex = ~operation;
                                if (readIndex < hitCount) {
                                    int keyIndex = readKeyIndex(config.seed, readIndex, readSet.keys.length);
                                    long before = System.nanoTime();
                                    byte[] actual = cache.get(section, readSet.keys[keyIndex]);
                                    readHitLatencies[readIndex] = System.nanoTime() - before;

                                    assertBytes(readSet.values[keyIndex], actual,
                                            "Concurrent read returned invalid bytes for read " + readIndex
                                                    + ", key index " + keyIndex);
                                    localReadBytes += actual.length;
                                } else {
                                    String missingKey = key("miss", config.seed, readIndex, MISS_KEY_DOMAIN);
                                    long before = System.nanoTime();
                                    byte[] actual = cache.get(section, missingKey);
                                    readMissLatencies[readIndex - hitCount] = System.nanoTime() - before;
                                    assertNull(actual,
                                            "Concurrent miss returned bytes for read " + readIndex);
                                }
                            } else {
                                int writeIndex = operation;
                                String writeKey = key("write", config.seed, writeIndex, WRITE_KEY_DOMAIN);
                                byte[] writeValue = value(config, writeIndex, WRITE_VALUE_DOMAIN);
                                long before = System.nanoTime();
                                cache.put(section, writeKey, writeValue);
                                writeLatencies[writeIndex] = System.nanoTime() - before;
                                localWriteBytes += writeValue.length;
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        threadReadBytes[threadIndex] = localReadBytes;
                        threadWriteBytes[threadIndex] = localWriteBytes;
                    }
                }
            });
        }

        long startNanos;
        try {
            ready.await();
            startNanos = System.nanoTime();
            start.countDown();

            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
            start.countDown();
            pool.shutdownNow();
            throw t;
        } finally {
            pool.shutdown();
        }

        long elapsedNanos = System.nanoTime() - startNanos;
        Throwable workerFailure = failure.get();
        if (workerFailure != null) {
            throw new RuntimeException("Concurrent workload failed", workerFailure);
        }

        long readBytes = 0;
        long writeBytes = 0;
        for (int thread = 0; thread < config.threadCount; thread++) {
            readBytes += threadReadBytes[thread];
            writeBytes += threadWriteBytes[thread];
        }

        return new WorkloadResult(elapsedNanos, readHitLatencies, readMissLatencies,
                writeLatencies, readBytes, writeBytes);
    }

    private static int[] createOperations(Config config) {
        int[] operations = new int[config.readCount + config.writeCount];
        for (int i = 0; i < config.readCount; i++) {
            operations[i] = ~i;
        }
        for (int i = 0; i < config.writeCount; i++) {
            operations[config.readCount + i] = i;
        }

        Random random = new Random(mix64(config.seed ^ OPERATION_ORDER_DOMAIN));
        for (int i = operations.length - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            int temp = operations[i];
            operations[i] = operations[j];
            operations[j] = temp;
        }
        return operations;
    }

    private static void validateWrittenValues(
            RioritaFsByteCache cache, String section, Config config, int verifyCount) {
        for (int i = 0; i < verifyCount; i++) {
            int writeIndex;
            if (verifyCount == config.writeCount) {
                writeIndex = i;
            } else if (verifyCount == 1) {
                writeIndex = config.writeCount / 2;
            } else {
                writeIndex = (int) ((long) i * (config.writeCount - 1L) / (verifyCount - 1L));
            }

            String writeKey = key("write", config.seed, writeIndex, WRITE_KEY_DOMAIN);
            byte[] expected = value(config, writeIndex, WRITE_VALUE_DOMAIN);
            assertBytes(expected, cache.get(section, writeKey),
                    "Post-run verification failed for write " + writeIndex);
        }
    }

    private static void printWorkloadResult(WorkloadResult result) {
        long operationCount = (long) result.readHitLatencies.length
                + result.readMissLatencies.length + result.writeLatencies.length;
        double seconds = result.elapsedNanos / 1_000_000_000.0;

        System.out.printf(Locale.ROOT,
                "Load completed: %,d operations in %.3f ms; %,.0f ops/s (end-to-end).%n",
                operationCount, nanosToMillis(result.elapsedNanos), operationCount / seconds);
        printLatency("Read hit", result.readHitLatencies);
        printLatency("Read miss", result.readMissLatencies);
        printLatency("Write", result.writeLatencies);
        System.out.printf(Locale.ROOT,
                "Payload throughput: read %s (%s/s), wrote %s (%s/s).%n%n",
                formatBytes(result.readBytes), formatBytesPerSecond(result.readBytes, seconds),
                formatBytes(result.writeBytes), formatBytesPerSecond(result.writeBytes, seconds));
    }

    private static void printLatency(String operation, long[] latencies) {
        if (latencies.length == 0) {
            System.out.println(operation + " latency: no operations.");
            return;
        }

        Arrays.sort(latencies);
        double sum = 0;
        for (long latency : latencies) {
            sum += latency;
        }

        System.out.printf(Locale.ROOT,
                "%s latency (cache call only): avg %s, p50 %s, p95 %s, p99 %s, max %s.%n",
                operation,
                formatNanos(sum / latencies.length),
                formatNanos(percentile(latencies, 0.50)),
                formatNanos(percentile(latencies, 0.95)),
                formatNanos(percentile(latencies, 0.99)),
                formatNanos(latencies[latencies.length - 1]));
    }

    private static long percentile(long[] sortedValues, double percentile) {
        int index = (int) Math.ceil(percentile * sortedValues.length) - 1;
        return sortedValues[Math.max(0, index)];
    }

    private static int readKeyIndex(long seed, int readIndex, int readKeyCount) {
        long mixed = mix64(seed ^ READ_CHOICE_DOMAIN ^ ((long) readIndex * 0x9E3779B97F4A7C15L));
        return (int) Math.floorMod(mixed, (long) readKeyCount);
    }

    private static String key(String prefix, long seed, int index, long domain) {
        long hash = mix64(seed ^ domain ^ ((long) index * 0x9E3779B97F4A7C15L));
        return prefix + '-' + Long.toHexString(hash) + '-' + Integer.toString(index, 36);
    }

    private static byte[] value(Config config, int index, long domain) {
        long valueSeed = mix64(config.seed ^ domain ^ ((long) index * 0xD1342543DE82EF95L));
        long sizeRange = (long) config.maxValueSize - config.minValueSize + 1L;
        int size = (int) (config.minValueSize + Math.floorMod(valueSeed, sizeRange));
        byte[] result = new byte[size];
        new Random(mix64(valueSeed ^ 0x94D049BB133111EBL)).nextBytes(result);
        return result;
    }

    private static long mix64(long value) {
        value = (value ^ (value >>> 30)) * 0xBF58476D1CE4E5B9L;
        value = (value ^ (value >>> 27)) * 0x94D049BB133111EBL;
        return value ^ (value >>> 31);
    }

    private static void assertBytes(byte[] expected, byte[] actual, String message) {
        if (!Arrays.equals(expected, actual)) {
            throw new AssertionError(message + ": " + describeDifference(expected, actual));
        }
    }

    private static void assertNull(byte[] actual, String message) {
        if (actual != null) {
            throw new AssertionError(message + ": got " + actual.length + " bytes");
        }
    }

    private static String describeDifference(byte[] expected, byte[] actual) {
        if (actual == null) {
            return "expected " + expected.length + " bytes, got null";
        }
        if (expected.length != actual.length) {
            return "expected " + expected.length + " bytes, got " + actual.length + " bytes";
        }
        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != actual[i]) {
                return "first difference at byte " + i + " (expected " + (expected[i] & 0xFF)
                        + ", got " + (actual[i] & 0xFF) + ')';
            }
        }
        return "unknown difference";
    }

    private static void printConfiguration(Config config, int readKeyCount, int verifyCount, String section) {
        System.out.println("Riorita load test");
        System.out.println("  readCount=" + config.readCount);
        System.out.println("  writeCount=" + config.writeCount);
        System.out.println("  valueSize=" + config.minValueSize + ".." + config.maxValueSize + " bytes");
        System.out.println("  threadCount=" + config.threadCount);
        System.out.println("  hitRate=" + config.hitRate + " (" + config.getHitCount()
                + " hits, " + config.getMissCount() + " misses)");
        System.out.println("  readKeyCount=" + readKeyCount);
        System.out.println("  verifyCount=" + verifyCount);
        System.out.println("  seed=" + config.seed);
        System.out.println("  directory=" + config.directory.getAbsolutePath());
        System.out.println("  section=" + section);
        System.out.println("  keepData=" + config.keepData);
        System.out.println();
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  Main readCount writeCount minValueSize maxValueSize threadCount [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --seed=N             deterministic data/order seed (default: 1)");
        System.out.println("  --directory=PATH     cache directory (default: ./tmp)");
        System.out.println("  --hit-rate=0..1      fraction of reads that must hit (default: 0.9)");
        System.out.println("  --read-keys=N        number of prefilled hot read keys");
        System.out.println("  --verify-count=N     verify N evenly-spaced writes after the run");
        System.out.println("  --verify-count=all   verify every write (default)");
        System.out.println("  --keep-data          do not remove the test section at the end");
        System.out.println("  --help               show this help");
    }

    private static String formatNanos(double nanos) {
        if (nanos < 1_000.0) {
            return String.format(Locale.ROOT, "%.0f ns", nanos);
        }
        if (nanos < 1_000_000.0) {
            return String.format(Locale.ROOT, "%.3f us", nanos / 1_000.0);
        }
        return String.format(Locale.ROOT, "%.3f ms", nanos / 1_000_000.0);
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024L) {
            return bytes + " B";
        }
        if (bytes < 1024L * 1024L) {
            return String.format(Locale.ROOT, "%.2f KiB", bytes / 1024.0);
        }
        if (bytes < 1024L * 1024L * 1024L) {
            return String.format(Locale.ROOT, "%.2f MiB", bytes / (1024.0 * 1024.0));
        }
        return String.format(Locale.ROOT, "%.2f GiB", bytes / (1024.0 * 1024.0 * 1024.0));
    }

    private static String formatBytesPerSecond(long bytes, double seconds) {
        return formatBytes((long) (bytes / seconds));
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static int parseNonNegativeInt(String name, String value) {
        int parsed;
        try {
            parsed = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer: " + value, e);
        }
        if (parsed < 0) {
            throw new IllegalArgumentException(name + " must be non-negative, got " + parsed);
        }
        return parsed;
    }

    private static final class Config {
        private int readCount;
        private int writeCount;
        private int minValueSize;
        private int maxValueSize;
        private int threadCount;
        private long seed = 1L;
        private double hitRate = 0.9;
        private File directory = new File("./tmp");
        private Integer readKeyCount;
        private Integer verifyCount;
        private boolean keepData;
        private boolean help;

        private static Config parse(String[] args) {
            Config result = new Config();
            if (args.length == 1 && "--help".equals(args[0])) {
                result.help = true;
                return result;
            }
            if (args.length < 5) {
                throw new IllegalArgumentException("Expected five positional arguments");
            }

            result.readCount = parseNonNegativeInt("readCount", args[0]);
            result.writeCount = parseNonNegativeInt("writeCount", args[1]);
            result.minValueSize = parseNonNegativeInt("minValueSize", args[2]);
            result.maxValueSize = parseNonNegativeInt("maxValueSize", args[3]);
            result.threadCount = parseNonNegativeInt("threadCount", args[4]);

            for (int i = 5; i < args.length; i++) {
                String arg = args[i];
                if (arg.startsWith("--seed=")) {
                    try {
                        result.seed = Long.parseLong(arg.substring("--seed=".length()));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("seed must be a long integer: " + arg, e);
                    }
                } else if (arg.startsWith("--directory=")) {
                    String path = arg.substring("--directory=".length());
                    if (path.isEmpty()) {
                        throw new IllegalArgumentException("directory path must not be empty");
                    }
                    result.directory = new File(path);
                } else if (arg.startsWith("--hit-rate=")) {
                    try {
                        result.hitRate = Double.parseDouble(arg.substring("--hit-rate=".length()));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("hit-rate must be a number: " + arg, e);
                    }
                } else if (arg.startsWith("--read-keys=")) {
                    result.readKeyCount = parseNonNegativeInt(
                            "read-keys", arg.substring("--read-keys=".length()));
                } else if (arg.startsWith("--verify-count=")) {
                    String value = arg.substring("--verify-count=".length());
                    if (!"all".equals(value)) {
                        result.verifyCount = parseNonNegativeInt("verify-count", value);
                    }
                } else if ("--keep-data".equals(arg)) {
                    result.keepData = true;
                } else if ("--help".equals(arg)) {
                    result.help = true;
                } else {
                    throw new IllegalArgumentException("Unknown option: " + arg);
                }
            }

            result.validate();
            return result;
        }

        private void validate() {
            long operationCount = (long) readCount + writeCount;
            if (operationCount == 0) {
                throw new IllegalArgumentException("readCount + writeCount must be positive");
            }
            if (operationCount > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Too many operations: " + operationCount);
            }
            if (maxValueSize < minValueSize) {
                throw new IllegalArgumentException("maxValueSize must be >= minValueSize");
            }
            if (threadCount <= 0) {
                throw new IllegalArgumentException("threadCount must be positive");
            }
            if (!Double.isFinite(hitRate) || hitRate < 0.0 || hitRate > 1.0) {
                throw new IllegalArgumentException("hit-rate must be between 0 and 1, got " + hitRate);
            }
            if (getHitCount() > 0 && readKeyCount != null && readKeyCount <= 0) {
                throw new IllegalArgumentException("read-keys must be positive when hit reads are requested");
            }
            if (verifyCount != null && verifyCount > writeCount) {
                throw new IllegalArgumentException(
                        "verify-count must not exceed writeCount (" + writeCount + ")");
            }
        }

        private int getReadKeyCount() {
            int hitCount = getHitCount();
            if (hitCount == 0) {
                return 0;
            }
            if (readKeyCount != null) {
                return Math.min(readKeyCount, hitCount);
            }

            long averageValueSize = minValueSize + ((long) maxValueSize - minValueSize) / 2L;
            long sizeLimitedCount = averageValueSize == 0
                    ? MAX_DEFAULT_READ_KEY_COUNT
                    : Math.max(1L, DEFAULT_READ_SET_BYTES / averageValueSize);
            return (int) Math.min(hitCount, Math.min(MAX_DEFAULT_READ_KEY_COUNT, sizeLimitedCount));
        }

        private int getVerifyCount() {
            return verifyCount == null ? writeCount : verifyCount;
        }

        private int getHitCount() {
            return (int) Math.round(readCount * hitRate);
        }

        private int getMissCount() {
            return readCount - getHitCount();
        }
    }

    private static final class ReadSet {
        private final String[] keys;
        private final byte[][] values;

        private ReadSet(String[] keys, byte[][] values) {
            this.keys = keys;
            this.values = values;
        }
    }

    private static final class WorkloadResult {
        private final long elapsedNanos;
        private final long[] readHitLatencies;
        private final long[] readMissLatencies;
        private final long[] writeLatencies;
        private final long readBytes;
        private final long writeBytes;

        private WorkloadResult(
                long elapsedNanos,
                long[] readHitLatencies,
                long[] readMissLatencies,
                long[] writeLatencies,
                long readBytes,
                long writeBytes) {
            this.elapsedNanos = elapsedNanos;
            this.readHitLatencies = readHitLatencies;
            this.readMissLatencies = readMissLatencies;
            this.writeLatencies = writeLatencies;
            this.readBytes = readBytes;
            this.writeBytes = writeBytes;
        }
    }
}
