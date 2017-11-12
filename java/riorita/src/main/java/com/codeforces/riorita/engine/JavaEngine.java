package com.codeforces.riorita.engine;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
public class JavaEngine implements Engine {
    private Map<String, Map<String, Value>> cache = new HashMap<>();

    @Override
    public boolean has(String section, String key) {
        Map<String, Value> sectionCache = cache.get(section);
        if (sectionCache == null) {
            return false;
        }

        Value value = sectionCache.get(key);
        if (value == null) {
            return false;
        }

        if (value.expirationTimeMillis <= System.currentTimeMillis()) {
            sectionCache.remove(key, value);
            return false;
        } else {
            return true;
        }
    }

    @Override
    public byte[] get(String section, String key) {
        Map<String, Value> sectionCache = cache.get(section);
        if (sectionCache == null) {
            return null;
        }

        Value value = sectionCache.get(key);
        if (value == null) {
            return null;
        }

        if (value.expirationTimeMillis <= System.currentTimeMillis()) {
            sectionCache.remove(key, value);
            return null;
        } else {
            return value.bytes;
        }
    }

    @Override
    public boolean put(String section, String key, byte[] bytes, long lifetimeMillis, boolean overwrite) {
        Map<String, Value> sectionCache = cache.get(section);
        if (sectionCache == null) {
            cache.put(section, new HashMap<>());
            sectionCache = cache.get(section);
        }

        Value value = sectionCache.get(key);
        long currentTimeMillis = System.currentTimeMillis();
        if (overwrite || (value == null || value.expirationTimeMillis <= currentTimeMillis)) {
            value = null;
        }

        if (value == null) {
            sectionCache.put(key, new Value(bytes, currentTimeMillis + lifetimeMillis));
            return true;
        }

        return false;
    }

    @Override
    public boolean erase(String section, String key) {
        Map<String, Value> sectionCache = cache.get(section);
        return sectionCache != null && sectionCache.remove(key) != null;
    }

    @Override
    public void erase(String section) {
        cache.remove(section);
    }

    @Override
    public void clear() {
        cache = new HashMap<>();
    }

    private static class Value {
        private final byte[] bytes;
        private final long expirationTimeMillis;

        private Value(byte[] bytes, long expirationTimeMillis) {
            this.bytes = Arrays.copyOf(bytes, bytes.length);
            this.expirationTimeMillis = expirationTimeMillis;
        }
    }
}
