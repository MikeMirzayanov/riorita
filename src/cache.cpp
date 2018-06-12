#include "cache.h"
#include "logger.h"

using namespace std;
using namespace riorita;

Logger logger("/opt/riorita/cache.log");

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
        logger << "Size: " << size << ", entries: " << keysByTimestamp.size()
            << " " << timestampsByKey.size() << " " << values.size() << endl; 
            
        auto timestampAndKey = keysByTimestamp.begin();
        size -= timestampAndKey->second.length();
        auto keyAndValue = values.find(timestampAndKey->second);
        size -= keyAndValue->second.length();

        logger << "Erase " << keyAndValue->first << " [" << timestampAndKey->first
                << " / " << timestamp << "]" << endl;

        keysByTimestamp.erase(timestampAndKey);
        timestampsByKey.erase(keyAndValue->first);
        values.erase(keyAndValue);
    }

    logger << "Size: " << size << ", entries: " << keysByTimestamp.size()
            << " " << timestampsByKey.size() << " " << values.size() << endl; 
}

bool Cache::has(const std::string& key)
{
    if (key.length() > MAX_CACHE_ENTRY_SIZE)
        return false;

    std::lock_guard<std::mutex> guard(lock);
    timestamp++;

    auto keyAndValue = values.find(key);
    if (keyAndValue == values.end())
        return false;
    else
    {
        renewTimestamp(key);
        return true;
    }
}

bool Cache::get(const std::string& key, std::string& value)
{
    if (key.length() > MAX_CACHE_ENTRY_SIZE)
        return false;

    std::lock_guard<std::mutex> guard(lock);
    timestamp++;

    auto keyAndValue = values.find(key);
    if (keyAndValue == values.end())
        return false;
    else
    {
        value = keyAndValue->second;
        renewTimestamp(key);
        return true;
    }
}

void Cache::put(const std::string& key, const std::string& value)
{
    if (key.length() + value.length() > MAX_CACHE_ENTRY_SIZE)
        return;

    std::lock_guard<std::mutex> guard(lock);
    timestamp++;

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
}

void Cache::erase(const std::string& key)
{
    if (key.length() > MAX_CACHE_ENTRY_SIZE)
        return;

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
}
