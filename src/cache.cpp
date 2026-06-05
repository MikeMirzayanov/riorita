#include "cache.h"
#include "logger.h"

#include <chrono>

using namespace std;
using namespace riorita;

Logger logger("/opt/riorita/cache.log");

size_t Cache::MAX_CACHE_ENTRY_SIZE = size_t(256) * 1024 * 1024;
size_t Cache::MAX_CACHE_SIZE = size_t(16) * 1024 * 1024 * 1024;

const long long CACHE_SUMMARY_INTERVAL_MILLIS = 60 * 1000;

static long long currentTimeMillis()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

void Cache::renewTimestamp(const std::string& key)
{
    size_t keyTimestamp = timestampsByKey[key];
    if (keyTimestamp > 0)
        keysByTimestamp.erase(keyTimestamp);
    
    keyTimestamp = timestamp;
    keysByTimestamp[keyTimestamp] = key;
    timestampsByKey[key] = keyTimestamp;
}

void Cache::removeOutdated()
{
    while (size > MAX_CACHE_SIZE)
    {
        auto timestampAndKey = keysByTimestamp.begin();
        auto keyAndValue = values.find(timestampAndKey->second);
        size_t removedBytes = timestampAndKey->second.length() + keyAndValue->second.length();
        size -= removedBytes;
        evictionCount++;
        evictedBytes += removedBytes;

        keysByTimestamp.erase(timestampAndKey);
        timestampsByKey.erase(keyAndValue->first);
        values.erase(keyAndValue);
    }
}

void Cache::resetSummaryCounters()
{
    getHitCount = 0;
    getMissCount = 0;
    hasHitCount = 0;
    hasMissCount = 0;
    putCount = 0;
    skippedTooLargeCount = 0;
    evictionCount = 0;
    evictedBytes = 0;
}

void Cache::maybeLogSummary()
{
    long long now = currentTimeMillis();
    if (lastSummaryMillis == 0)
    {
        lastSummaryMillis = now;
        return;
    }

    long long intervalMillis = now - lastSummaryMillis;
    if (intervalMillis < CACHE_SUMMARY_INTERVAL_MILLIS)
        return;

    logger << "Cache summary {size=" << size
        << ", entries=" << values.size()
        << ", getHit=" << getHitCount
        << ", getMiss=" << getMissCount
        << ", hasHit=" << hasHitCount
        << ", hasMiss=" << hasMissCount
        << ", puts=" << putCount
        << ", skippedTooLarge=" << skippedTooLargeCount
        << ", evictions=" << evictionCount
        << ", evictedBytes=" << evictedBytes
        << ", intervalMillis=" << intervalMillis
        << "}" << endl;
    logger << std::flush;

    resetSummaryCounters();
    lastSummaryMillis = now;
}

bool Cache::has(const std::string& key)
{
    if (key.length() > MAX_CACHE_ENTRY_SIZE)
    {
        std::lock_guard<std::mutex> guard(lock);
        hasMissCount++;
        maybeLogSummary();
        return false;
    }

    std::lock_guard<std::mutex> guard(lock);
    timestamp++;

    auto keyAndValue = values.find(key);
    if (keyAndValue == values.end())
    {
        hasMissCount++;
        maybeLogSummary();
        return false;
    }
    else
    {
        hasHitCount++;
        renewTimestamp(key);
        maybeLogSummary();
        return true;
    }
}

bool Cache::get(const std::string& key, std::string& value)
{
    if (key.length() > MAX_CACHE_ENTRY_SIZE)
    {
        std::lock_guard<std::mutex> guard(lock);
        getMissCount++;
        maybeLogSummary();
        return false;
    }

    std::lock_guard<std::mutex> guard(lock);
    timestamp++;

    auto keyAndValue = values.find(key);
    if (keyAndValue == values.end())
    {
        getMissCount++;
        maybeLogSummary();
        return false;
    }
    else
    {
        getHitCount++;
        value = keyAndValue->second;
        renewTimestamp(key);
        maybeLogSummary();
        return true;
    }
}

void Cache::put(const std::string& key, const std::string& value)
{
    if (key.length() + value.length() > MAX_CACHE_ENTRY_SIZE)
    {
        std::lock_guard<std::mutex> guard(lock);
        skippedTooLargeCount++;
        maybeLogSummary();
        return;
    }

    std::lock_guard<std::mutex> guard(lock);
    timestamp++;
    putCount++;

    auto keyAndValue = values.find(key);
    if (keyAndValue == values.end())
        size += key.length() + value.length();
    else
    {
        size -= keyAndValue->second.length();
        size += value.length();
    }

    values[key] = value;
    renewTimestamp(key);
    removeOutdated();
    maybeLogSummary();
}

void Cache::erase(const std::string& key)
{
    if (key.length() > MAX_CACHE_ENTRY_SIZE)
    {
        std::lock_guard<std::mutex> guard(lock);
        maybeLogSummary();
        return;
    }

    std::lock_guard<std::mutex> guard(lock);

    auto keyAndValue = values.find(key);
    if (keyAndValue != values.end())
    {
        size -= keyAndValue->first.length();
        size -= keyAndValue->second.length();
    
        auto keyAndTimestamp = timestampsByKey.find(key);
        keysByTimestamp.erase(keyAndTimestamp->second);
        timestampsByKey.erase(keyAndTimestamp);
        values.erase(keyAndValue);
    }
    maybeLogSummary();
}
