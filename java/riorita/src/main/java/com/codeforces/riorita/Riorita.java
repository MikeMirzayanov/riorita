package com.codeforces.riorita;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class Riorita {
    private static final Logger logger = Logger.getLogger(Riorita.class);

    private static final byte MAGIC_BYTE = 113;
    private static final byte PROTOCOL_VERSION = 1;
    private static final int MAX_RECONNECT_COUNT = 10;
    private static final long WARN_THRESHOLD_MILLIS = 100;

    private static final int RECEIVE_BUFFER_SIZE = 1048576;
    private static final int SEND_BUFFER_SIZE = 1048576;

    private final Random random = new Random(Riorita.class.hashCode() ^ this.hashCode());

    private Socket socket;
    private final SocketAddress socketAddress;
    private InputStream inputStream;
    private OutputStream outputStream;

    private final String hostAndPort;
    private final boolean reconnect;

    public Riorita(String host, int port) throws IOException {
        this(host, port, true);
    }

    public Riorita(String host, int port, boolean reconnect) throws IOException {
        this.hostAndPort = host + ":" + port;
        this.reconnect = reconnect;
        socketAddress = new InetSocketAddress(host, port);
    }

    private void reconnectQuietly() {
        if (socket != null) {
            try {
                logger.info("Closing socket [" + hostAndPort + "].");
                socket.close();
            } catch (IOException e) {
                // No operations.
            }
        }

        try {
            socket = new Socket();
            socket.setReceiveBufferSize(RECEIVE_BUFFER_SIZE);
            socket.setSendBufferSize(SEND_BUFFER_SIZE);
            socket.setTcpNoDelay(true);

            socket.connect(socketAddress);

            inputStream = socket.getInputStream();
            outputStream = socket.getOutputStream();

            logger.info("Connected to " + hostAndPort + ".");
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

    private <T> T runOperation(Operation<T> operation, int size) throws IOException {
        long startTimeMills = System.currentTimeMillis();

        try {
            if (!reconnect) {
                T result = operation.run();
                if (result instanceof byte[]) {
                    size += ((byte[]) result).length;
                }
                return result;
            } else {
                IOException exception = null;
                int iteration = 0;

                while (iteration < MAX_RECONNECT_COUNT) {
                    if (socket == null || !socket.isConnected() || socket.isClosed()) {
                        logger.warn("socket == null || !socket.isConnected() || socket.isClosed()");
                        reconnectQuietly();
                    }

                    iteration++;

                    if (socket != null && socket.isConnected() && !socket.isClosed()) {
                        try {
                            T result = operation.run();
                            if (result instanceof byte[]) {
                                size += ((byte[]) result).length;
                            }
                            return result;
                        } catch (IOException e) {
                            logger.warn("Can't process operation.", e);
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

                throw exception == null ? new IOException("Can't connect to " + hostAndPort + ".") : exception;
            }
        } finally {
            long duration = System.currentTimeMillis() - startTimeMills;

            if (duration > WARN_THRESHOLD_MILLIS) {
                logger.warn("Operation " + operation.getType() + " takes " + duration + " ms, id=" + operation.getRequestId() + " [size=" + size + " bytes, " + hostAndPort + "].");
                // System.out.println("Operation " + operation.getType() + " takes " + duration + " ms, id=" + operation.getRequestId() + " [size=" + size + " bytes].");
            } else {
                logger.info("Operation " + operation.getType() + " takes " + duration + " ms, id=" + operation.getRequestId() + " [size=" + size + " bytes, " + hostAndPort + "].");
                // System.out.println("Operation " + operation.getType() + " takes " + duration + " ms, id=" + operation.getRequestId() + " [size=" + size + " bytes].");
            }
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

            @Override
            public Type getType() {
                return Type.PING;
            }

            @Override
            public long getRequestId() {
                return requestId;
            }
        }, 0);
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
                outputStream.flush();

                int responseLength = readResponseLength(requestId);
                if (responseLength != 16) {
                    throw new IOException("Expected exactly 16 bytes in response [requestId=" + requestId + "].");
                }

                return readResponseVerdict(requestId);
            }

            @Override
            public Type getType() {
                return Type.HAS;
            }

            @Override
            public long getRequestId() {
                return requestId;
            }
        }, keyBytes.length);
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
                outputStream.flush();

                int responseLength = readResponseLength(requestId);
                if (responseLength != 16) {
                    throw new IOException("Expected exactly 16 bytes in response [requestId=" + requestId + "].");
                }

                return readResponseVerdict(requestId);
            }

            @Override
            public Type getType() {
                return Type.DELETE;
            }

            @Override
            public long getRequestId() {
                return requestId;
            }
        }, keyBytes.length);
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
                outputStream.flush();

                int responseLength = readResponseLength(requestId);
                if (responseLength != 16) {
                    throw new IOException("Expected exactly 16 bytes in response [requestId=" + requestId + "].");
                }

                return readResponseVerdict(requestId);
            }

            @Override
            public Type getType() {
                return Type.PUT;
            }

            @Override
            public long getRequestId() {
                return requestId;
            }
        }, keyBytes.length + bytes.length);
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
                outputStream.flush();

                int responseLength = readResponseLength(requestId);
                if (responseLength < 16) {
                    throw new IOException("Expected at least 16 bytes in response, but " + responseLength + " found [requestId=" + requestId + "].");
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

            @Override
            public Type getType() {
                return Type.GET;
            }

            @Override
            public long getRequestId() {
                return requestId;
            }
        }, keyBytes.length);
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
        Type getType();
        long getRequestId();
    }
}
