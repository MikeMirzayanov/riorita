#include "storage.h"

#include <string>
#include <map>
#include <cstdlib>
#include <cstdio>

#include <boost/filesystem.hpp>

#ifdef HAS_LEVELDB
#   include "leveldb/db.h"
#   include "leveldb/cache.h"
#endif

using namespace riorita;
using namespace std;

namespace riorita {

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
        int done = fread(bytes, 1, size, f);
        if (done != size)
        {
            delete[] bytes;
            return false;
        }

        bytes[done] = 0;
        value = bytes;
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

        FILE* f = fopen(fileName.c_str(), "wb");
        fwrite(value.c_str(), 1, value.length(), f);
        fclose(f);
    }

private:
    StorageOptions options;

    string getFileName(const string& key)
    {
        return options.directory + '/' + key + ".bin";
    }
};

#ifdef HAS_LEVELDB

struct LevelDbStorage: public Storage
{
    LevelDbStorage(const StorageOptions& options)
    {
        this->options.block_cache = leveldb::NewLRUCache(1024 * 1048576);
        this->options.create_if_missing = true;
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


Storage* newStorage(StorageType type, const StorageOptions& options)
{
    if (type == MEMORY)
        return new MemoryStorage(options);

    if (type == FILES)
        return new FilesStorage(options);

#ifdef HAS_LEVELDB
    if (type == LEVELDB)
        return new LevelDbStorage(options);
#endif

    return 0;
}

}
