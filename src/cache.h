#include <string>
#include <map>
#include <unordered_map>
#include <mutex>
#include <iostream>

namespace riorita {

class Cache {
private:
    std::mutex lock;
    size_t timestamp = 0;
    size_t size = 0;
    std::map<size_t, std::string> keysByTimestamp;
    std::map<std::string, size_t> timestampsByKey;
    std::unordered_map<std::string, std::string> values;
    long long lastSummaryMillis = 0;
    unsigned long long getHitCount = 0;
    unsigned long long getMissCount = 0;
    unsigned long long hasHitCount = 0;
    unsigned long long hasMissCount = 0;
    unsigned long long putCount = 0;
    unsigned long long skippedTooLargeCount = 0;
    unsigned long long evictionCount = 0;
    unsigned long long evictedBytes = 0;

    void renewTimestamp(const std::string& key);
    void removeOutdated();
    void maybeLogSummary();
    void resetSummaryCounters();

public:
    static size_t MAX_CACHE_ENTRY_SIZE;
    static size_t MAX_CACHE_SIZE;

    bool has(const std::string& key);
    bool get(const std::string& key, std::string& value);
    void put(const std::string& key, const std::string& value);
    void erase(const std::string& key);
};

}
