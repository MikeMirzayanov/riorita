#ifndef RIORITA_COMPACT_H_
#define RIORITA_COMPACT_H_

#include <string>
#include <map>
#include <cstdlib>

#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

namespace riorita {

struct Position
{
    int index;
    int offset;
    int length;
    int fingerprint;
};

class FileSystemCompactStorage
{
public:
    FileSystemCompactStorage(const std::string& dir);
    bool get(const std::string& name, std::string& data);
    bool has(const std::string& name);
    void put(const std::string& name, const std::string& data);
    void erase(const std::string& name);

private:
    void readIndexFile();
    void appendNameAndPosition(const std::string& name, const Position& position);
    void prepareDataFile(int index);
    void put(int index, const std::string& data, int fp);

    std::string dir;
    std::map<std::string, Position> positionByName;
    int index;
    int offset;
    boost::mutex mutex;
};

}

#endif
