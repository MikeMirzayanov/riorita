#include "compact.h"

#include <cstdio>
#include <cstring>
#include <cassert>
#include <stdexcept>
#include <boost/filesystem.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

using namespace riorita;
using namespace std;

const string INDEX_FILE = "riorita.index";
const string DATA_FILE_PATTERN = "riorita.%04d";

const size_t BLOCK_SIZE = 1024 * 1024;
const size_t DATA_FILE_SIZE = 1024 * 1024 * 1024;
const size_t MAX_DATA_FILE_NAME_LENGTH = 64;
const size_t INT_SIZE = int(sizeof(int));
const size_t POSITION_SIZE = sizeof(Position);

static int getGroupBySectionAndName(const string& section, const string& name, int groups) {
    int result = 0;
    for (size_t i = 0; i < section.length(); i++)
        result = (result * 113 + int(int(section[i]) + 255)) % 1061599;
    for (size_t i = 0; i < name.length(); i++)
        result = (result * 1009 + int(int(name[i]) + 255)) % 1062599;
    return result % groups;
}

static int fingerprint(const char* c, size_t size) {
    int result = 0;
    for (size_t i = 0; i < size; i++)
        result = result * 97 + int(int(c[i]) + 255);
    return result;
}

static string concatPath(const string& dir, const string& child) {
#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
    return dir + "\\" + child;
#else
    return dir + "/" + child;
#endif
}

FileSystemCompactStorage::FileSystemCompactStorage(const string& dir, int groups)
        : groups(groups), dir(dir), closed(false) {
    assert(POSITION_SIZE == 32);
    if (groups <= 0)
        throw runtime_error("Riorita: group count must be positive");

    boost::filesystem::create_directory(dir);

    mutexes.resize(groups);
    resetInMemoryIndex();

    if (!readIndexFile())
        recoverFromCorruption("broken index file");
}

void Position::erase() {
    this->group = this->index = this->offset = this->length = 0;
    this->fingerprint = 1;
    this->expiration_timestamp = 0LL;
}

static bool isErasedOrOutdated(const Position& position, timestamp current_timestamp) {
    return (position.group == 0 && position.index == 0 && position.offset == 0
        && position.length == 0 && position.fingerprint == 1) || position.expiration_timestamp <= current_timestamp;
}

static bool readInt(const string& data, size_t& pos, int& value) {
    if (pos + INT_SIZE > data.length())
        return false;
    memcpy(&value, data.c_str() + pos, INT_SIZE);
    pos += INT_SIZE;
    return true;
}

static bool readBytes(const string& data, size_t& pos, int length, string& value) {
    if (length < 0 || pos + size_t(length) > data.length())
        return false;
    value.assign(data.c_str() + pos, size_t(length));
    pos += size_t(length);
    return true;
}

static bool readPosition(const string& data, size_t& pos, Position& position) {
    if (pos + POSITION_SIZE > data.length())
        return false;
    memcpy(&position, data.c_str() + pos, POSITION_SIZE);
    pos += POSITION_SIZE;
    return true;
}

static bool isValidPosition(const Position& position, int groups) {
    long long nextOffset = (long long)position.offset + (long long)position.length + (long long)INT_SIZE;
    return position.group >= 0 && position.group < groups
        && position.index >= 0
        && position.offset >= 0
        && position.length >= 0
        && nextOffset >= 0
        && nextOffset <= (long long)DATA_FILE_SIZE;
}

bool FileSystemCompactStorage::has(const string& section, const string& name, timestamp current_timestamp) {
    boost::unique_lock<boost::mutex> scoped_lock(mutex);

    if (positionBySectionAndName.count(section)) {
        auto& positionByName = positionBySectionAndName[section];
        return positionByName.count(name) && !isErasedOrOutdated(positionByName[name], current_timestamp);
    } else
        return false;
}

void FileSystemCompactStorage::erase(const string& section) {
    boost::unique_lock<boost::mutex> scoped_lock(mutex);

    if (positionBySectionAndName.count(section)) {
        Position position = {0, 0, 0, 0, 1, 0LL};
        for (auto& p: positionBySectionAndName[section]) {
            p.second = position;
            appendSectionNameAndPosition(section, p.first, p.second);
        }
    }
}

bool FileSystemCompactStorage::erase(const string& section, const string& name, timestamp current_timestamp) {
    boost::unique_lock<boost::mutex> scoped_lock(mutex);

    if (positionBySectionAndName.count(section)) {
        auto& positionByName = positionBySectionAndName[section];
        if (positionByName.count(name)) {
            Position& position = positionByName[name];
            if (!isErasedOrOutdated(position, current_timestamp)) {
                position.erase();
                appendSectionNameAndPosition(section, name, position);
                return true;
            }
        }
    }

    return false;
}

bool FileSystemCompactStorage::get(const string& section, const string& name, timestamp current_timestamp, string& data) {
    data.clear();
    Position position = {0, 0, 0, 0, 1, 0LL};
    bool result = false;
    bool corrupted = false;
    string corruptionReason;

    {
        boost::unique_lock<boost::mutex> scoped_lock(mutex);
        if (positionBySectionAndName.count(section)) {
            auto& positionByName = positionBySectionAndName[section];
            if (positionByName.count(name))
                position = positionByName[name];
        }
    }

    if (isErasedOrOutdated(position, current_timestamp))
        return false;

    char groupName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(groupName, "%d", position.group);
    char fileName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(fileName, DATA_FILE_PATTERN.c_str(), position.index);
    char* bytes = 0;

    {
        boost::unique_lock<boost::mutex> scoped_lock(mutexes[position.group]);
        string filePath = concatPath(dir, concatPath(groupName, fileName));
        FILE* f = fopen(filePath.c_str(), "rb");
        if (0 != f) {
            if (0 == fseek(f, position.offset, SEEK_SET)) {
                bytes = new char[position.length + INT_SIZE];
                result = (position.length + INT_SIZE == fread(bytes, 1, position.length + INT_SIZE, f));
                if (!result) {
                    corrupted = true;
                    corruptionReason = "broken fread";
                }
            } else {
                corrupted = true;
                corruptionReason = "unable to seek";
            }
            fclose(f);
        } else {
            corrupted = true;
            corruptionReason = "unable to open data file";
        }
    }

    if (result) {
        int fp;
        memcpy(&fp, bytes + position.length, INT_SIZE);
        result = (position.fingerprint == fingerprint(bytes, position.length) && position.fingerprint == fp);
        if (!result) {
            corrupted = true;
            corruptionReason = "broken fingerprint";
        }
    }

    if (result)
        data.append(bytes, position.length);

    if (0 != bytes)
        delete[] bytes; 

    if (corrupted) {
        recoverFromCorruption(corruptionReason);
        data.clear();
        return false;
    }

    return result;
}

void FileSystemCompactStorage::prepareDataFile(int group, int index) {
    char groupName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(groupName, "%d", group);
    boost::filesystem::path groupDir(concatPath(dir, groupName));
    boost::filesystem::create_directory(groupDir);

    char fileName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(fileName, DATA_FILE_PATTERN.c_str(), index);
    
    FILE* f = fopen(concatPath(dir, concatPath(groupName, fileName)).c_str(), "wb");
    if (0 != f)
        fclose(f);
}

void FileSystemCompactStorage::put(int group, int index, const string& data, int fp) {
    char groupName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(groupName, "%d", group);
    char fileName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(fileName, DATA_FILE_PATTERN.c_str(), index);
    
    FILE* f = fopen(concatPath(dir, concatPath(groupName, fileName)).c_str(), "ab");
    if (0 != f) {
        fwrite(data.c_str(), 1, data.length(), f);
        fwrite(&fp, 1, INT_SIZE, f);
        fclose(f);
    } else
        throw runtime_error("Riorita: unable to open file to put");
}

bool FileSystemCompactStorage::put(const string& section, const string& name, const string& data,
        timestamp current_timestamp, timestamp lifetime, bool overwrite) {
    if (closed)
        return false;
    
    int group = getGroupBySectionAndName(section, name, groups);

    {
        boost::unique_lock<boost::mutex> scoped_lock(mutexes[group]);

        if (!overwrite && has(section, name, current_timestamp))
            return false;
        
        if (offsets[group] + data.length() + INT_SIZE >= DATA_FILE_SIZE) {
            indices[group]++;
            offsets[group] = 0;
            prepareDataFile(group, indices[group]);
        }

        int fp = fingerprint(data.c_str(), data.length());
        Position position = {group, indices[group], offsets[group], int(data.length()), fp, current_timestamp + lifetime};
        put(group, indices[group], data, fp);
        
        {
            boost::unique_lock<boost::mutex> scoped_lock(mutex);
            positionBySectionAndName[section][name] = position;
            appendSectionNameAndPosition(section, name, position);
        }
        
        offsets[group] += int(data.length() + INT_SIZE);
    }

    return true;
}

void FileSystemCompactStorage::appendSectionNameAndPosition(const string& section, const string& name, const Position& position) {
    string indexFile = concatPath(dir, INDEX_FILE);
    FILE* indexFilePtr = fopen(indexFile.c_str(), "a+b");
    
    if (0 != indexFilePtr) {
        size_t size = INT_SIZE + section.length() + INT_SIZE + name.length() + POSITION_SIZE;
        char* data = new char[size];
        size_t off = 0;
        
        int length = int(section.length());
        memcpy(data + off, &length, INT_SIZE);
        off += INT_SIZE;
        memcpy(data + off, section.c_str(), section.length());
        off += section.length();

        length = int(name.length());
        memcpy(data + off, &length, INT_SIZE);
        off += INT_SIZE;
        memcpy(data + off, name.c_str(), name.length());
        off += name.length();

        memcpy(data + off, &position, POSITION_SIZE);
        off += POSITION_SIZE;

        fwrite(data, 1, size, indexFilePtr);
        delete[] data;
        fclose(indexFilePtr);
    }
}

char block[BLOCK_SIZE];
bool FileSystemCompactStorage::readIndexFile() {
    boost::unique_lock<boost::mutex> scoped_lock(mutex);
    string indexFile = concatPath(dir, INDEX_FILE);
    FILE* indexFilePtr = fopen(indexFile.c_str(), "rb");

    bool hasError = false;
    bool hasEof = false;
    string indexData;
    
    if (0 != indexFilePtr) {
        while (true) {
            size_t read = fread(block, 1, BLOCK_SIZE, indexFilePtr);
            
            if (read > 0)
                indexData.append(block, read);
            
            if (read != BLOCK_SIZE) {
                hasError = ferror(indexFilePtr);
                hasEof = feof(indexFilePtr);
                break;
            }
        }

        fclose(indexFilePtr);

        if (hasError || !hasEof)
            return false;
        
        size_t pos = 0;
        while (pos < indexData.length()) {
            int sectionLength;
            if (!readInt(indexData, pos, sectionLength))
                return false;

            string section;
            if (!readBytes(indexData, pos, sectionLength, section))
                return false;

            int nameLength;
            if (!readInt(indexData, pos, nameLength))
                return false;

            string name;
            if (!readBytes(indexData, pos, nameLength, name))
                return false;

            Position position;
            if (!readPosition(indexData, pos, position) || !isValidPosition(position, groups))
                return false;

            positionBySectionAndName[section][name] = position;

            if (position.index > indices[position.group]) {
                indices[position.group] = position.index;
                offsets[position.group] = int(position.offset + position.length + INT_SIZE);
            }

            if (position.index == indices[position.group])
                offsets[position.group] = max(offsets[position.group], int(position.offset + position.length + INT_SIZE));
        }
    }

    return true;
}   

void FileSystemCompactStorage::resetInMemoryIndex() {
    positionBySectionAndName.clear();
    indices = vector<int>(groups, -1);
    offsets = vector<int>(groups, DATA_FILE_SIZE);
}

void FileSystemCompactStorage::clearWithoutLocks() {
    boost::filesystem::create_directory(dir);

    for (boost::filesystem::directory_iterator end, i(dir); i != end; ++i) {
        boost::filesystem::remove_all(i->path());
    }

    resetInMemoryIndex();
}

void FileSystemCompactStorage::clear() {
    boost::ptr_vector<boost::unique_lock<boost::mutex>> groupLocks;
    for (int group = 0; group < groups; group++)
        groupLocks.push_back(new boost::unique_lock<boost::mutex>(mutexes[group]));

    boost::unique_lock<boost::mutex> scoped_lock(mutex);
    if (!closed)
        clearWithoutLocks();
}

void FileSystemCompactStorage::recoverFromCorruption(const string& reason) {
    fprintf(stderr, "Riorita: corrupted storage detected [dir=%s, reason=%s], clearing directory and continuing with empty data.\n",
            dir.c_str(), reason.c_str());
    fflush(stderr);
    clear();
}

void FileSystemCompactStorage::close() {
    boost::ptr_vector<boost::unique_lock<boost::mutex>> groupLocks;
    for (int group = 0; group < groups; group++)
        groupLocks.push_back(new boost::unique_lock<boost::mutex>(mutexes[group]));

    boost::unique_lock<boost::mutex> scoped_lock(mutex);
    if (!this->closed) {
        this->closed = true;
        clearWithoutLocks();
    }
}

const string FileSystemCompactStorage::get_dir() {
    return this->dir;
}

int FileSystemCompactStorage::get_groups() {
    return this->groups;
}
