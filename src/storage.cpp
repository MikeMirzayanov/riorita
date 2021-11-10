#include "storage.h"

#include <string>
#include <map>
#include <cstdlib>
#include <cstdio>
#include "snappy.h"
#include "compact.h"

#include <boost/filesystem.hpp>

#ifdef HAS_LEVELDB
#   include "leveldb/db.h"
#   include "leveldb/cache.h"
#   include "leveldb/filter_policy.h"
#endif

#ifdef HAS_ROCKSDB
#include <rocksdb/db.h>
#endif

using namespace riorita;
using namespace std;

namespace riorita {

StorageType getType(const string& typeName)
{
    if (typeName == "memory" || typeName == "MEMORY")
        return MEMORY;

    if (typeName == "files" || typeName == "FILES")
        return FILES;

    if (typeName == "leveldb" || typeName == "LEVELDB")
        return LEVELDB;

    if (typeName == "compact" || typeName == "COMPACT")
        return COMPACT;

    if (typeName == "rocksdb" || typeName == "ROCKSDB")
        return LEVELDB;

    return ILLEGAL_STORAGE_TYPE;
}

struct MemoryStorage: public Storage
{
    MemoryStorage(const StorageOptions& options)
    {
        // No operations.
    }

    bool has(const string& key)
    {
        return data.count(key) != 0;
    }

    bool get(const string& key, string& value)
    {
        if (data.count(key) != 0)
        {
            value = data[key];
            return true;
        }
        else
            return false;
    }

    void erase(const string& key)
    {
        data.erase(key);
    }

    void put(const string& key, const string& value)
    {
        data[key] = value;
    }

private:
    map<string, string> data;
};

// ==============================================================================

struct FilesStorage: public Storage
{
    FilesStorage(const StorageOptions& options): options(options)
    {
        // No operations.
    }

    bool has(const string& key)
    {
        return boost::filesystem::exists(getFileName(key));
    }

    bool get(const string& key, string& value)
    {
        string fileName = getFileName(key);
        FILE* f = fopen(fileName.c_str(), "rb");
        if (!f)
            return false;
        
        int size = int(boost::filesystem::file_size(fileName));

        char* bytes = new char[size + 1];
        int done = int(fread(bytes, 1, size, f));
        if (done != size)
        {
            delete[] bytes;
            return false;
        }

        bytes[done] = 0;
        snappy::Uncompress(bytes, done, &value);
        delete[] bytes;
        fclose(f);

        return true;
    }

    void erase(const string& key)
    {
        boost::filesystem::remove(getFileName(key));
    }

    void put(const string& key, const string& value)
    {
        string fileName = getFileName(key);

        size_t lastSlash = fileName.find_last_of('/');
        if (lastSlash != string::npos)
        {
            string dirs = fileName.substr(0, lastSlash);
            boost::filesystem::create_directories(dirs);
        }

        string compressed;
        snappy::Compress(value.data(), value.size(), &compressed);

        FILE* f = fopen(fileName.c_str(), "wb");
        fwrite(compressed.c_str(), 1, compressed.length(), f);
        fclose(f);
    }

private:
    StorageOptions options;

    string getFileName(const string& key)
    {
        string path;
        for (size_t i = 2; i <= 6; i+=2)
            if (key.size() >= i)
                path += key.substr(i - 2, 2) + '/';
        return options.directory + '/' + path + key + ".bin";
    }
};

// ==============================================================================

struct CompactStorage: public Storage
{
    CompactStorage(const StorageOptions& options)
    {
        boost::filesystem::create_directories(options.directory);
        compact = new FileSystemCompactStorage(options.directory, 8);
    }

    ~CompactStorage()
    {
        delete compact;
    }

    bool has(const string& key)
    {
        return compact->has(key);
    }

    bool get(const string& key, string& value)
    {
        string raw;
        bool result = compact->get(key, raw);
        if (result)
            snappy::Uncompress(raw.data(), raw.size(), &value);
        return result;
    }

    void erase(const string& key)
    {
        compact->erase(key);
    }

    void put(const string& key, const string& value)
    {
        string raw;
        snappy::Compress(value.data(), value.size(), &raw);
        compact->put(key, raw);
    }

private:
    FileSystemCompactStorage* compact;
};

// ==============================================================================

#ifdef HAS_LEVELDB

struct LevelDbStorage: public Storage
{
    LevelDbStorage(const StorageOptions& options)
    {
        this->options.block_cache = leveldb::NewLRUCache(128 * 1024);
        this->options.create_if_missing = true;
        this->options.write_buffer_size = 64 * 1024 * 1024;
        this->options.block_size = 65536;
        this->options.filter_policy = leveldb::NewBloomFilterPolicy(10);
        leveldb::Status status = leveldb::DB::Open(this->options, options.directory, &db);
        assert(status.ok());
    }

    bool has(const string& key)
    {
        std::string value;
        leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &value);
        return s.ok();
    }

    bool get(const string& key, string& value)
    {
        leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &value);
        return s.ok();
    }

    void erase(const string& key)
    {
        db->Delete(leveldb::WriteOptions(), key);
    }

    void put(const string& key, const string& value)
    {
        db->Put(leveldb::WriteOptions(), key, value);
    }

    ~LevelDbStorage()
    {
        delete db;

        if (options.block_cache)
            delete options.block_cache;
    }

private:
    leveldb::DB* db;
    leveldb::Options options;
};

#endif

#ifdef HAS_ROCKSDB
struct RocksDBStorage: public Storage {

   RocksDBStorage(const StorageOptions& options)
    {
        this->options.create_if_missing = true;
        this->options.create_missing_column_families = true;
	this->options.allow_mmap_reads = true;
	this->options.allow_mmap_writes = true;
        this->options.write_buffer_size = 64 * 1024 * 1024;
        const auto status = rocksdb::DB::Open(this->options, options.directory, &db);
        assert(status.ok());
    }

    bool has(const string& key)
    {
        std::string value;
        const auto s = db->Get(rocksdb::ReadOptions(), key, &value);
        return s.ok();
    }

    bool get(const string& key, string& value)
    {
        const auto s = db->Get(rocksdb::ReadOptions(), key, &value);
        return s.ok();
    }

    void erase(const string& key)
    {
        db->Delete(rocksdb::WriteOptions(), key);
    }

    void put(const string& key, const string& value)
    {
        db->Put(rocksdb::WriteOptions(), key, value);
    }

   ~RocksDBStorage()
    {
        delete db;
    }

private:
    rocksdb::DB* db;
    rocksdb::Options options;
};
#endif

Storage* newStorage(StorageType type, const StorageOptions& options)
{

    switch (type) {
      case MEMORY:
        return new MemoryStorage(options);
      case FILES:
        return new FilesStorage(options);
#ifdef HAS_LEVELDB
      case LEVELDB:
        return new LevelDbStorage(options);
#endif
      case COMPACT:
        return new CompactStorage(options);
#ifdef HAS_ROCKSDB
      case ROCKSDB:
        return new RocksDBStorage(options);
#endif
    }

    return 0;
}

}
