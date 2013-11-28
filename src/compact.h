#ifndef RIORITA_COMPACT_H_
#define RIORITA_COMPACT_H_

#include <string>
#include <map>
#include <cstdlib>

namespace riorita {

struct Position
{
    std::size_t index;
    std::size_t offset;
    std::size_t length;
    std::size_t fingerprint;
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
    void put(std::size_t index, const std::string& data, std::size_t fp);

    std::string dir;
    std::map<std::string, Position> positionByName;
    size_t index;
    size_t offset;
};

}

#endif
