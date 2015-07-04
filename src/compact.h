#ifndef RIORITA_COMPACT_H_
#define RIORITA_COMPACT_H_

#include <string>
#include <map>
#include <cstdlib>

#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

namespace riorita {

struct Position
{   
    int group;
    int index;
    int offset;
    int length;
    int fingerprint;
};

class FileSystemCompactStorage
{
public:
    FileSystemCompactStorage(const std::string& dir, int groups);
    bool get(const std::string& name, std::string& data);
    bool has(const std::string& name);
    void put(const std::string& name, const std::string& data);
    void erase(const std::string& name);

private:
    void readIndexFile();
    void appendNameAndPosition(const std::string& name, const Position& position);
    void prepareDataFile(int group, int index);
    void put(int group, int index, const std::string& data, int fp);

    int groups;
    std::string dir;
    std::map<std::string, Position> positionByName;

    std::vector<int> indices;
    std::vector<int> offsets;
    boost::ptr_vector<boost::mutex> mutexes;
    
    boost::mutex mutex;
};

}

#endif
