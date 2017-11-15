package com.codeforces.riorita.engine;

import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.Arrays;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
public class RioritaEngine implements Engine {
    private static int INSTANCE_COUNT = 0;
    private final long id;

    public RioritaEngine(File dataDir) {
        INSTANCE_COUNT++;
        id = 13 * (13 * RioritaEngine.class.getClassLoader().hashCode() + RioritaEngine.class.hashCode())
                + INSTANCE_COUNT;
        initialize(dataDir.getAbsolutePath(), 8);
    }

    @SuppressWarnings("SameParameterValue")
    private native void initialize(String absolutePath, int groupCount);

    @Override
    public boolean has(String section, String key) {
        return has(section, key, System.currentTimeMillis());
    }

    private native boolean has(String section, String key, long currentTimestamp);

    @Override
    public byte[] get(String section, String key) {
        byte[] bytes = get(section, key, System.currentTimeMillis());
        if (bytes != null) {
            try {
                bytes = Snappy.uncompress(bytes);
            } catch (IOException e) {
                throw new RuntimeException("Can't uncompress bytes.", e);
            }
        }
        return bytes;
    }

    private native byte[] get(String section, String key, long currentTimestamp);

    @Override
    public boolean put(String section, String key, byte[] bytes, long lifetimeMillis, boolean overwrite) {
        try {
            return put(section, key, Snappy.compress(bytes), System.currentTimeMillis(), lifetimeMillis, overwrite);
        } catch (IOException e) {
            throw new RuntimeException("Can't compress bytes.", e);
        }
    }

    private native boolean put(String section, String key, byte[] bytes, long current_timestamp,
                               long lifetime, boolean overwrite);

    @Override
    public boolean erase(String section, String key) {
        return erase(section, key, System.currentTimeMillis());
    }

    private native boolean erase(String section, String key, long current_timestamp);

    @Override
    public native void erase(String section);

    @Override
    public native void clear();

    @SuppressWarnings("SameParameterValue")
    private static void loadLibraryFromJar(String name) throws IOException {
        InputStream in = RioritaEngine.class.getResourceAsStream("/" + name);

        ByteArrayOutputStream byteArrayOutputStream
                = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = in.read(buffer)) != -1) {
            byteArrayOutputStream.write(buffer, 0, read);
        }
        byteArrayOutputStream.close();
        in.close();

        byte[] bytes = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        String filename = Math.abs(Arrays.hashCode(bytes)) + "_" + bytes.length + "_" + name;

        File temp = File.createTempFile(name, "");
        File tempDir = temp.getParentFile();
        if (!temp.delete()) {
            throw new IOException("Can't delete temporary file " + temp + ".");
        }

        File file = new File(tempDir, filename);
        if (!file.exists() || file.length() != bytes.length) {
            FileOutputStream outputStream = new FileOutputStream(file);
            outputStream.write(bytes);
            outputStream.close();
        }
        System.load(file.getAbsolutePath());
    }

    @Override
    public String toString() {
        return "RioritaEngine{" +
                "id=" + id +
                '}';
    }

    static {
        String os = System.getProperty("os.name").toLowerCase();

        try {
            if (os.contains("linux")) {
                loadLibraryFromJar("riorita_engine.so");
            } else if (os.contains("windows")) {
                loadLibraryFromJar("riorita_engine.so");
            } else {
                throw new RuntimeException("Expected 'linux' or 'windows' os, but found '" + os + "'.");
            }
        } catch (IOException e) {
            throw new RuntimeException("Can't load riorita_engine native library.", e);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println(System.nanoTime());
        Thread.sleep(1000);
        System.out.println(System.nanoTime());


//        {
//            Engine engine = new RioritaEngine(new File("C:\\Temp\\1\\" + System.currentTimeMillis()));
//            byte[] a = new byte[]{};
//            System.out.println(engine.put("a", "b", a, 10000000, false));
//            System.out.println(Arrays.toString(engine.get("a", "b")));
//        }
        {
            Engine engine = new JavaEngine();
            byte[] a = new byte[]{};
            System.out.println(engine.put("thisistest", "2128596", a, 10000000, true));
            System.out.println(Arrays.toString(engine.get("thisistest", "2128596")));
        }

    }
}

