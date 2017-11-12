package com.codeforces.riorita.engine;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
public interface Engine {
    boolean has(String section, String key);
    byte[] get(String section, String key);
    boolean put(String section, String key, byte[] bytes, long lifetimeMillis, boolean overwrite);
    boolean erase(String section, String key);
    void erase(String section);
    void clear();
}
