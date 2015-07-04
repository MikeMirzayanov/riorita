#include "compact.h"

#include <cstdio>
#include <cstring>
#include <cassert>
#include <boost/filesystem.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
// #include <windows.h>

using namespace riorita;
using namespace std;

const string INDEX_FILE = "FileSystemCompactStorage.index";
const string DATA_FILE_PATTERN = "FileSystemCompactStorage.%04d";
const int BLOCK_SIZE = 1024 * 1024;
const int DATA_FILE_SIZE = 1024 * 1024 * 1024;
const int MAX_DATA_FILE_NAME_LENGTH = 64;
const int INT_SIZE = int(sizeof(int));

static int getGroupByName(const string& name, int groups)
{
    int result = 0;
    for (int i = 0; i < name.length(); i++)
        result = (result * 1009 + int(int(name[i]) + 255)) % 1062599;
    return result % groups;
}

static int fingerprint(const char* c, int size)
{
    int result = 0;
    for (int i = 0; i < size; i++)
        result = result * 97 + int(int(c[i]) + 255);
    return result;
}

static string concatPath(const string& dir, const string& child)
{
#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
    return dir + "\\" + child;
#else
    return dir + "/" + child;
#endif
}

FileSystemCompactStorage::FileSystemCompactStorage(const string& dir, int groups)
        : groups(groups), dir(dir)
{
    indices = vector<int>(groups, -1);
    offsets = vector<int>(groups, DATA_FILE_SIZE);
    mutexes.resize(groups);

    readIndexFile();
}

static bool isErased(const Position& position)
{
    return position.group == 0 && position.index == 0 && position.offset == 0
        && position.length == 0 && position.fingerprint == 1;
}

bool FileSystemCompactStorage::has(const string& name)
{
    boost::unique_lock<boost::mutex> scoped_lock(mutex);

    return positionByName.count(name)
        && !isErased(positionByName[name]);
}

void FileSystemCompactStorage::erase(const string& name)
{
    boost::unique_lock<boost::mutex> scoped_lock(mutex);
    
    if (has(name))
    {
        Position position = {0, 0, 0, 0, 1};
        positionByName[name] = position;
        appendNameAndPosition(name, position);
    }
}

bool FileSystemCompactStorage::get(const string& name, string& data)
{
    data.clear();
    Position position = {0, 0, 0, 0, 1};
    bool result = false;

    {
        boost::unique_lock<boost::mutex> scoped_lock(mutex);
        if (positionByName.count(name))
            position = positionByName[name];
    }

    if (isErased(position))
        return false;

    char groupName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(groupName, "%d", position.group);
    char fileName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(fileName, DATA_FILE_PATTERN.c_str(), position.index);
    char* bytes = 0;

    {
        boost::unique_lock<boost::mutex> scoped_lock(mutexes[position.group]);
        FILE* f = fopen(concatPath(dir, concatPath(groupName, fileName)).c_str(), "rb");
        if (0 != f)
        {
            if (0 == fseek(f, position.offset, SEEK_SET))
            {
                bytes = new char[position.length + INT_SIZE];
                result = (position.length + INT_SIZE == fread(bytes, 1, position.length + INT_SIZE, f));
                if (!result)
                    printf("Broken fread\n");
            }
            else
                printf("Can't seek\n");
            fclose(f);
        }
        else
            printf("f == 0\n");
    }

    if (result)
    {
        int fp;
        memcpy(&fp, bytes + position.length, INT_SIZE);
        result = (position.fingerprint == fingerprint(bytes, position.length)
                && position.fingerprint == fp);
        if (!result)
            printf("Broken fps: %d %d %d\n", position.fingerprint, fingerprint(bytes, position.length), fp);
    }

    if (result)
        data.append(bytes, position.length);

    if (0 != bytes)
        delete[] bytes; 

    if (!result)
        printf("!result\n");

    return result;
}

void FileSystemCompactStorage::prepareDataFile(int group, int index)
{
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

void FileSystemCompactStorage::put(int group, int index, const string& data, int fp)
{
    char groupName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(groupName, "%d", group);
    char fileName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(fileName, DATA_FILE_PATTERN.c_str(), index);
    
    FILE* f = fopen(concatPath(dir, concatPath(groupName, fileName)).c_str(), "ab");
    if (0 != f)
    {
        fwrite(data.c_str(), 1, data.length(), f);
        fwrite(&fp, 1, INT_SIZE, f);
        fclose(f);
    }
}

void FileSystemCompactStorage::put(const string& name, const string& data)
{
    int group = getGroupByName(name, groups);

    {
        boost::unique_lock<boost::mutex> scoped_lock(mutexes[group]);
        
        if (offsets[group] + int(data.length() + INT_SIZE) >= DATA_FILE_SIZE)
        {
            indices[group]++;
            offsets[group] = 0;
            prepareDataFile(group, indices[group]);
        }

        int fp = fingerprint(data.c_str(), data.length());
        Position position = {group, indices[group], offsets[group], int(data.length()), fp};
        put(group, indices[group], data, fp);
        
        {
            boost::unique_lock<boost::mutex> scoped_lock(mutex);
            positionByName[name] = position;
            appendNameAndPosition(name, position);
        }
        
        offsets[group] += data.length() + INT_SIZE;
    }
}

void FileSystemCompactStorage::appendNameAndPosition(const string& name, const Position& position)
{
    string indexFile = concatPath(dir, INDEX_FILE);
    FILE* indexFilePtr = fopen(indexFile.c_str(), "a+b");
    if (0 != indexFilePtr)
    {
        int size = INT_SIZE + name.length() + sizeof(Position);
        char* data = new char[size];
        int length = name.length();
        memcpy(data, &length, INT_SIZE);
        memcpy(data + INT_SIZE, name.c_str(), name.length());
        memcpy(data + INT_SIZE + name.length(), &position, sizeof(Position));
        fwrite(data, 1, size, indexFilePtr);
        delete[] data;
    }
    fclose(indexFilePtr);
}

void FileSystemCompactStorage::readIndexFile()
{
    boost::unique_lock<boost::mutex> scoped_lock(mutex);

    string indexFile = concatPath(dir, INDEX_FILE);
    FILE* indexFilePtr = fopen(indexFile.c_str(), "rb");
    
    bool hasError = false;
    bool hasEof = false;
    string indexData;
    
    if (0 != indexFilePtr)
    {
        char block[BLOCK_SIZE];
        
        while (true)
        {
            int read = fread(block, 1, BLOCK_SIZE, indexFilePtr);
            
            if (read > 0)
                indexData.append(block, read);
            
            if (read != BLOCK_SIZE)
            {
                hasError = ferror(indexFilePtr);
                hasEof = feof(indexFilePtr);
                
                break;
            }
        }
        
        assert(sizeof(Position) == 20);

        int pos = 0;
        while (pos < int(indexData.length()) && !hasError && hasEof)
        {
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

            positionByName[name] = position;

            if (position.index > indices[position.group])
            {
                indices[position.group] = position.index;
                offsets[position.group] = position.offset + position.length + INT_SIZE;
            }

            if (position.index == indices[position.group])
                offsets[position.group] = max(offsets[position.group], position.offset + position.length + INT_SIZE);
        }

        fclose(indexFilePtr);
    }
}   
