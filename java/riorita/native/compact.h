#ifndef RIORITA_COMPACT_H_
#define RIORITA_COMPACT_H_

#include <string>
#include <map>
#include <cstdlib>

#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

namespace riorita {

typedef long long timestamp;

struct Position {   
    int group;
    int index;
    int offset;
    int length;
    int fingerprint;
    timestamp expiration_timestamp;

    void erase();
};

class FileSystemCompactStorage {
public:
    FileSystemCompactStorage(const std::string& dir, int groups);
    bool get(const std::string& section, const std::string& name, timestamp current_timestamp, std::string& data);
    bool has(const std::string& section, const std::string& name, timestamp current_timestamp);
    bool put(const std::string& section, const std::string& name, const std::string& data,
        timestamp current_timestamp, timestamp lifetime, bool overwrite);
    bool erase(const std::string& section, const std::string& name, timestamp current_timestamp);
    void erase(const std::string& section);
    void close();
    const std::string get_dir();
    int get_groups();

private:
    void readIndexFile();
    void appendSectionNameAndPosition(const std::string& section, const std::string& name, const Position& position);
    void prepareDataFile(int group, int index);
    void put(int group, int index, const std::string& data, int fp);

    int groups;
    std::string dir;
    std::map<std::string, std::map<std::string, Position>> positionBySectionAndName;

    std::vector<int> indices;
    std::vector<int> offsets;
    boost::ptr_vector<boost::mutex> mutexes;
    
    bool closed;
    boost::mutex mutex;
};

}

#endif
