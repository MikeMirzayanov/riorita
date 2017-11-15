#include "compact.h"

#include <cstdio>
#include <cstring>
#include <cassert>
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

    boost::filesystem::create_directory(dir);

    indices = vector<int>(groups, -1);
    offsets = vector<int>(groups, DATA_FILE_SIZE);
    mutexes.resize(groups);

    readIndexFile();
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
        FILE* f = fopen(concatPath(dir, concatPath(groupName, fileName)).c_str(), "rb");
        if (0 != f) {
            if (0 == fseek(f, position.offset, SEEK_SET)) {
                bytes = new char[position.length + INT_SIZE];
                result = (position.length + INT_SIZE == fread(bytes, 1, position.length + INT_SIZE, f));
                if (!result)
                    throw runtime_error("Riorita: broken fread");
            } else
                throw runtime_error("Riorita: unable to seek");
            fclose(f);
        } else
            throw runtime_error("Riorita: unable to open data file");
    }

    if (result) {
        int fp;
        memcpy(&fp, bytes + position.length, INT_SIZE);
        result = (position.fingerprint == fingerprint(bytes, position.length) && position.fingerprint == fp);
        if (!result)
            throw runtime_error("Riorita: broken fingerprint");
    }

    if (result)
        data.append(bytes, position.length);

    if (0 != bytes)
        delete[] bytes; 

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
    }

    fclose(indexFilePtr);
}

char block[BLOCK_SIZE];
void FileSystemCompactStorage::readIndexFile() {
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
        
        size_t pos = 0;
        while (pos < indexData.length() && !hasError && hasEof)
        {
            int sectionLength;
            memcpy(&sectionLength, indexData.c_str() + pos, INT_SIZE);
            pos += INT_SIZE;
            string section(indexData.c_str() + pos, sectionLength);
            pos += sectionLength;

            int nameLength;
            memcpy(&nameLength, indexData.c_str() + pos, INT_SIZE);
            pos += INT_SIZE;
            string name(indexData.c_str() + pos, nameLength);
            pos += nameLength;

            Position position;
            memcpy(&position, indexData.c_str() + pos, sizeof(Position));
            assert(position.group >= 0);
            assert(position.group < groups);
            pos += sizeof(Position);

            positionBySectionAndName[section][name] = position;

            if (position.index > indices[position.group]) {
                indices[position.group] = position.index;
                offsets[position.group] = int(position.offset + position.length + INT_SIZE);
            }

            if (position.index == indices[position.group])
                offsets[position.group] = max(offsets[position.group], int(position.offset + position.length + INT_SIZE));
        }

        fclose(indexFilePtr);
    }
}   

void FileSystemCompactStorage::close() {
    boost::unique_lock<boost::mutex> scoped_lock(mutex);

    if (!this->closed) {
        this->closed = true;
        for (boost::filesystem::directory_iterator end, i(dir); i != end; ++i) {
            boost::filesystem::remove_all(i->path());
        }
    }
}

const string FileSystemCompactStorage::get_dir() {
    return this->dir;
}

int FileSystemCompactStorage::get_groups() {
    return this->groups;
}
