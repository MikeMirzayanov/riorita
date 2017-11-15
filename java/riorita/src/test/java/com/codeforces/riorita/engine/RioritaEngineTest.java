package com.codeforces.riorita.engine;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
public class RioritaEngineTest extends TestCase {
    private Random random = new Random(1);

    private String generateRandomString(Set<String> set) {
        while (true) {
            int length = random.nextInt(10) + 1;
            StringBuilder s = new StringBuilder();
            for (int i = 0; i < length; i++) {
                s.append((char) ('a' + random.nextInt(5)));
            }
            String ss = s.toString();
            if (!set.contains(ss)) {
                set.add(ss);
                return ss;
            }
        }
    }

    private List<String> generateRandomStrings(int size) {
        Set<String> set = new HashSet<>();
        List<String> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(generateRandomString(set));
        }
        return result;
    }

    private Type getType(int[] p) {
        int sum = 0;
        for (int i : p) {
            sum += i;
        }
        int f = random.nextInt(sum);
        sum = 0;
        for (int i = 0; i < p.length; i++) {
            sum += p[i];
            if (f < sum) {
                return Type.values()[i];
            }
        }
        throw new RuntimeException();
    }

    private String any(List<String> s) {
        return s.get(random.nextInt(s.size()));
    }

    private void internalTestRioritaEngine(int iterationCount, int sectionCount, int keyCount, int averageLifetime, int averageValueLength, int[] hgpee) throws IOException {
        List<String> sections = generateRandomStrings(sectionCount);
        List<String> keys = generateRandomStrings(keyCount);

        File tmp = File.createTempFile("riorita", "" + System.currentTimeMillis());
        tmp.delete();
        tmp.mkdirs();

        try {
            Engine rio = new RioritaEngine(tmp);
            Engine exp = new JavaEngine();

            for (int i = 0; i < iterationCount; i++) {
                Type type = getType(hgpee);
                //System.out.println(i + " -> " + System.currentTimeMillis());

                if (type == Type.HAS) {
                    String section = any(sections);
                    String key = any(keys);
                    // System.out.println(type + " " + section + " " + key);
                    boolean r1 = rio.has(section, key);
                    boolean r2 = exp.has(section, key);
                    if (r1 != r2) {
                        throw new RuntimeException("Failed iteration " + i + ": has: found " + r1 + ", expected " + r2 + ".");
                    }
                }
                if (type == Type.GET) {
                    String section = any(sections);
                    String key = any(keys);
                    // System.out.println(type + " " + section + " " + key);
                    byte[] r1 = rio.get(section, key);
                    byte[] r2 = exp.get(section, key);
                    if (!Arrays.equals(r1, r2)) {
                        throw new RuntimeException("Failed iteration " + i + ": get: found " + Arrays.toString(r1) + ", expected " + Arrays.toString(r2) + ".");
                    }
                }
                if (type == Type.PUT) {
                    String section = any(sections);
                    String key = any(keys);
                    long lifetime = averageLifetime / 2 + random.nextInt(averageLifetime * 2);
                    int length = random.nextInt(averageValueLength * 2);
                    boolean overwrite = random.nextBoolean();

                    byte[] value = new byte[length];
                    for (int j = 0; j < length; j++) {
                        value[j] = (byte) random.nextInt(10);
                    }

                    // System.out.println(type + " " + section + " " + key + " " + value.length + " " + lifetime + " " + overwrite);

                    boolean r1 = rio.put(section, key, value, lifetime, overwrite);
                    boolean r2 = exp.put(section, key, value, lifetime, overwrite);

                    if (r1 != r2) {
                        throw new RuntimeException("Failed iteration " + i + ": put: found " + r1 + ", expected " + r2 + ".");
                    }
                }
                if (type == Type.E1) {
                    String section = any(sections);
                    // System.out.println(type + " " + section);
                    rio.erase(section);
                    exp.erase(section);
                }
                if (type == Type.E2) {
                    String section = any(sections);
                    String key = any(keys);
                    // System.out.println(type + " " + section + " " + key);
                    rio.erase(section, key);
                    exp.erase(section, key);
                }
            }
            System.out.println("Processed " + iterationCount + " queries.");
        } finally {
            delete(tmp);
        }
    }

    private void delete(File f) throws IOException {
        if (f.exists()) {
            if (f.isDirectory()) {
                File[] files = f.listFiles();
                if (files != null) {
                    for (File c : files)
                        delete(c);
                }
            }
            if (!f.delete()) {
                throw new IOException("Failed to delete the file '" + f + "'.");
            }
        }
    }

    public void testRioritaEngine() throws IOException {
//        internalTestRioritaEngine(10000, 2, 2, 200, 2, new int[] {20, 20, 20, 20, 20});
//        internalTestRioritaEngine(10000, 3, 3, 200, 3, new int[] {20, 20, 20, 20, 20});
//        internalTestRioritaEngine(10000, 4, 4, 200, 5000, new int[] {20, 20, 20, 20, 20});
        internalTestRioritaEngine(10000, 5, 5, 200, 100000, new int[]{20, 20, 20, 20, 20});
//        internalTestRioritaEngine(10000, 6, 6, 200, 600, new int[] {20, 20, 20, 20, 20});
//        internalTestRioritaEngine(10000, 7, 7, 200, 700, new int[] {20, 20, 20, 20, 20});
    }

    private enum Type {
        HAS,
        GET,
        PUT,
        E1,
        E2
    }
}
