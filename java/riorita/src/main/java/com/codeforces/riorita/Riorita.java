package com.codeforces.riorita;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class Riorita {
    private static final byte MAGIC_BYTE = 113;
    private static final byte PROTOCOL_VERSION = 1;
    private static final int MAX_RECONNECT_COUNT = 20;

    private final Random random = new Random(Riorita.class.hashCode() ^ this.hashCode());

    private Socket socket;
    private final SocketAddress socketAddress;
    private InputStream inputStream;
    private OutputStream outputStream;

    private final boolean reconnect;

    public Riorita(String host, int port) throws IOException {
        this(host, port, true);
    }

    public Riorita(String host, int port, boolean reconnect) throws IOException {
        this.reconnect = reconnect;
        socketAddress = new InetSocketAddress(host, port);
    }

    private void reconnectQuietly() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                // No operations.
            }
        }

        try {
            socket = new Socket();
            socket.connect(socketAddress);

            inputStream = socket.getInputStream();
            outputStream = socket.getOutputStream();
        } catch (IOException ignored) {
            // No operations.
        }
    }

    private void readExactly(byte[] bytes, int off, int size, long requestId) throws IOException {
        int done = 0;

        while (done < size) {
            int read = inputStream.read(bytes, off + done, size - done);

            if (read < 0) {
                throw new IOException("Unexpected end of inputStream [requestId=" + requestId + "].");
            }

            done += read;
        }
    }

    private <T> T runOperation(Operation<T> operation) throws IOException {
        if (!reconnect) {
            return operation.run();
        } else {
            IOException exception = null;
            int iteration = 0;

            while (iteration < MAX_RECONNECT_COUNT) {
                if (socket == null || !socket.isConnected() || socket.isClosed()) {
                    reconnectQuietly();
                }

                iteration++;

                if (socket != null && socket.isConnected() && !socket.isClosed()) {
                    try {
                        return operation.run();
                    } catch (IOException e) {
                        System.out.println(e);
                        exception = e;
                        try {
                            Thread.sleep(iteration * 100);
                        } catch (InterruptedException ignored) {
                            // No operations.
                        }
                        reconnectQuietly();
                    }
                } else {
                    try {
                        Thread.sleep(iteration * 100);
                    } catch (InterruptedException ignored) {
                        // No operations.
                    }
                }
            }

            throw exception == null ? new IOException("Can't connect to " + socketAddress + ".") : exception;
        }
    }

    private ByteBuffer newRequestBuffer(Type type, long requestId, int keyLength, Integer valueLength) {
        int requestLength = 4 // Request length.
                + 1 // Magic byte.
                + 1 // Protocol version.
                + 1 // Type.
                + 8 // Request id.
                + 4 // Key length.
                + keyLength // Key.
                + (valueLength != null ? 4 + valueLength : 0) // Value length + value.
                ;

        ByteBuffer byteBuffer = ByteBuffer.allocate(requestLength).order(ByteOrder.LITTLE_ENDIAN);

        byteBuffer.putInt(requestLength);
        byteBuffer.put(MAGIC_BYTE);
        byteBuffer.put(PROTOCOL_VERSION);
        byteBuffer.put(type.getByte());
        byteBuffer.putLong(requestId);
        byteBuffer.putInt(keyLength);

        return byteBuffer;
    }

    private int readResponseLength(long requestId) throws IOException {
        ByteBuffer responseLengthBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        readExactly(responseLengthBuffer.array(), 0, 4, requestId);
        return responseLengthBuffer.getInt();
    }

    private boolean readResponseVerdict(long requestId) throws IOException {
        int responseHeaderLength = 1 // Magic byte.
                + 1 // Protocol version.
                + 8 // Request id.
                + 1 // Success.
                + 1 // Verdict.
                ;

        ByteBuffer responseBuffer = ByteBuffer.allocate(responseHeaderLength).order(ByteOrder.LITTLE_ENDIAN);
        readExactly(responseBuffer.array(), 0, responseHeaderLength, requestId);

        int magicByte = responseBuffer.get();
        if (magicByte != MAGIC_BYTE) {
            throw new IOException("Invalid magic: expected " + (int) MAGIC_BYTE + ", found " + magicByte + " [requestId=" + requestId + "].");
        }

        int protocolVersion = responseBuffer.get();
        if (protocolVersion != PROTOCOL_VERSION) {
            throw new IOException("Invalid protocol: expected " + (int) PROTOCOL_VERSION + ", found " + protocolVersion + " [requestId=" + requestId + "].");
        }

        long receivedRequestId = responseBuffer.getLong();
        if (receivedRequestId != requestId) {
            throw new IOException("Invalid request id: expected " + requestId + ", found " + receivedRequestId + ".");
        }

        byte success = responseBuffer.get();
        if (success != 0 && success != 1) {
            throw new IOException("Operation returned illegal success " + success + " [requestId=" + requestId + "].");
        }
        if (success != 1) {
            throw new IOException("Operation didn't return with success [requestId=" + requestId + "].");
        }

        byte verdict = responseBuffer.get();
        if (verdict != 0 && verdict != 1) {
            throw new IOException("Operation returned illegal verdict " + verdict + " [requestId=" + requestId + "].");
        }

        return verdict == 1;
    }

    private long nextRequestId() {
        return Math.abs(random.nextLong() % 1000000000000000000L);
    }

    private byte[] getStringBytes(String s) {
        try {
            return s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Can't find UTF-8.");
        }
    }

    public boolean ping() throws IOException {
        final long requestId = nextRequestId();
        final ByteBuffer pingBuffer = newRequestBuffer(Type.PING, requestId, 0, null);

        return runOperation(new Operation<Boolean>() {
            @Override
            public Boolean run() throws IOException {
                outputStream.write(pingBuffer.array());
                outputStream.flush();

                int responseLength = readResponseLength(requestId);
                if (responseLength != 16) {
                    throw new IOException("Expected exactly 16 bytes in response [requestId=" + requestId + "].");
                }

                return readResponseVerdict(requestId);
            }
        });
    }

    public boolean has(String key) throws IOException {
        byte[] keyBytes = getStringBytes(key);

        final long requestId = nextRequestId();
        final ByteBuffer hasBuffer = newRequestBuffer(Type.HAS, requestId, keyBytes.length, null);
        hasBuffer.put(keyBytes);

        return runOperation(new Operation<Boolean>() {
            @Override
            public Boolean run() throws IOException {
                outputStream.write(hasBuffer.array());

                int responseLength = readResponseLength(requestId);
                if (responseLength != 16) {
                    throw new IOException("Expected exactly 16 bytes in response [requestId=" + requestId + "].");
                }

                return readResponseVerdict(requestId);
            }
        });
    }

    public boolean delete(String key) throws IOException {
        byte[] keyBytes = getStringBytes(key);

        final long requestId = nextRequestId();
        final ByteBuffer deleteBuffer = newRequestBuffer(Type.DELETE, requestId, keyBytes.length, null);
        deleteBuffer.put(keyBytes);

        return runOperation(new Operation<Boolean>() {
            @Override
            public Boolean run() throws IOException {
                outputStream.write(deleteBuffer.array());

                int responseLength = readResponseLength(requestId);
                if (responseLength != 16) {
                    throw new IOException("Expected exactly 16 bytes in response [requestId=" + requestId + "].");
                }

                return readResponseVerdict(requestId);
            }
        });
    }

    public boolean put(String key, byte[] bytes) throws IOException {
        byte[] keyBytes = getStringBytes(key);

        final long requestId = nextRequestId();
        final ByteBuffer putBuffer = newRequestBuffer(Type.PUT, requestId, keyBytes.length, bytes.length);
        putBuffer.put(keyBytes);
        putBuffer.putInt(bytes.length);
        putBuffer.put(bytes);

        return runOperation(new Operation<Boolean>() {
            @Override
            public Boolean run() throws IOException {
                outputStream.write(putBuffer.array());

                int responseLength = readResponseLength(requestId);
                if (responseLength != 16) {
                    throw new IOException("Expected exactly 16 bytes in response [requestId=" + requestId + "].");
                }

                return readResponseVerdict(requestId);
            }
        });
    }

    public byte[] get(String key) throws IOException {
        byte[] keyBytes = getStringBytes(key);

        final long requestId = nextRequestId();
        final ByteBuffer getBuffer = newRequestBuffer(Type.GET, requestId, keyBytes.length, null);
        getBuffer.put(keyBytes);

        return runOperation(new Operation<byte[]>() {
            @Override
            public byte[] run() throws IOException {
                outputStream.write(getBuffer.array());

                int responseLength = readResponseLength(requestId);
                if (responseLength < 16) {
                    throw new IOException("Expected at least 16 bytes in response [requestId=" + requestId + "].");
                }

                boolean verdict = readResponseVerdict(requestId);

                if (!verdict) {
                    if (responseLength != 16) {
                        throw new IOException("Expected exactly 16 bytes in response [requestId=" + requestId + "].");
                    }

                    return null;
                } else {
                    ByteBuffer valueLengthBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                    readExactly(valueLengthBuffer.array(), 0, 4, requestId);

                    int valueLength = valueLengthBuffer.getInt();
                    if (valueLength < 0) {
                        throw new IOException("Expected positive length of value in response [requestId=" + requestId + "].");
                    }

                    ByteBuffer valueBuffer = ByteBuffer.allocate(valueLength).order(ByteOrder.LITTLE_ENDIAN);
                    readExactly(valueBuffer.array(), 0, valueLength, requestId);
                    return valueBuffer.array();
                }
            }
        });
    }

    public enum Type {
        PING,
        HAS,
        GET,
        PUT,
        DELETE;

        byte getByte() {
            return (byte) (ordinal() + 1);
        }
    }

    private interface Operation<T> {
        T run() throws IOException;
    }

    private static final Random TEST_RANDOM = new Random(13);
    private static String getRandomString(int length) {
        StringBuilder result = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            result.append((char)('a' + TEST_RANDOM.nextInt(26)));
        }

        return result.toString();
    }

    private static void testSize(Riorita riorita, int total, int size) throws IOException {
        int iterations = total / size;

        System.out.println("Doing " + iterations + " iterations for size " + size + ".");

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

            result = getRandomString(size).getBytes();
            riorita.put(key, result);
            cache.put(key, result);
        }

        System.out.println("Completed in " + (System.currentTimeMillis() - start) + " ms.");
    }

    public static void main(String[] args) throws IOException {
        Riorita riorita = new Riorita("localhost", 8100);

//        testSize(riorita, 1000000, 100);
//        testSize(riorita, 10000000, 10000);
//        testSize(riorita, 100000000, 100000);
//        testSize(riorita, 100000000, 1000000);
//        testSize(riorita, 100000000, 10000000);
//        testSize(riorita, 200000000, 20000000);
//        testSize(riorita, 500000000, 50000000);
//        testSize(riorita, 500000000, 100000000);

        testSize(riorita, 1000000000, 1000);
        if (true) {
            return;
        }

        System.out.println(riorita.has("test"));
        System.out.println(riorita.get("test"));
        riorita.put("test", "tezt".getBytes());
        System.out.println(riorita.has("test"));
        System.out.println(new String(riorita.get("test")));
        riorita.delete("test");
        System.out.println(riorita.has("test"));
        System.out.println(riorita.get("test"));

        if (true) {
            return;
        }

        long start = System.currentTimeMillis();

        int tt = 0;
        for (int i = 0; i < 100000; i++) {
            riorita.put("test" + i, ("privet!" + i).getBytes());
            byte[] bytes = riorita.get("test" + i);
            //System.out.println(new String(bytes));
            //riorita.delete("test" + i);

            if (i % 1000 == 0) {
                System.out.println(i);
            }
        }

        System.out.println((System.currentTimeMillis() - start) + " ms");
    }
}
