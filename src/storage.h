#ifndef RIORITA_STORAGE_H_
#define RIORITA_STORAGE_H_

#include <string>

namespace riorita {

struct StorageOptions
{
    std::string directory;    
};

struct Storage
{
    virtual bool has(const std::string& key) = 0;
    virtual bool get(const std::string& key, std::string& value) = 0;
    virtual void erase(const std::string& key) = 0;
    virtual void put(const std::string& key, const std::string& value) = 0;
};

enum StorageType
{
    ILLEGAL_STORAGE_TYPE,
    MEMORY,
    FILES,
    LEVELDB
};

Storage* newStorage(StorageType type, const StorageOptions& options);

StorageType getType(const std::string& typeName);

}

#endif
