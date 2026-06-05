#include "compact.h"
#include "logger.h"

#include <cerrno>
#include <chrono>
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
const int SIZEOF_INT = int(sizeof(int));

extern boost::shared_ptr<riorita::Logger> lout;

static int getGroupByName(const string& name, int groups)
{
    size_t result = 0;
    for (size_t i = 0; i < name.length(); i++)
        result = (result * 977 + size_t(name[i] + 255));
    return int(result % groups);
}

static int fingerprint(const char* c, int size)
{
    size_t result = 0;
    for (int i = 0; i < size; i++)
        result = result * 977 + size_t(c[i] + 255);
    return int(result % 2147483647);
}

static string concatPath(const string& dir, const string& child)
{
#if defined(_WIN32) || defined(WIN32) || defined(_WIN64) || defined(WIN64)
    return dir + "\\" + child;
#else
    return dir + "/" + child;
#endif
}

static string errnoMessage(int errorNumber)
{
    if (errorNumber == 0)
        return "none";
    static boost::mutex strerrorMutex;
    boost::unique_lock<boost::mutex> scoped_lock(strerrorMutex);
    return strerror(errorNumber);
}

static void logFileOpenError(const string& operation, const string& filePath, int errorNumber)
{
    *lout << operation << " failed [file=" << filePath
        << ", errno=" << errorNumber
        << ", message=" << errnoMessage(errorNumber)
        << "]" << endl;
    *lout << std::flush;
}

static void logFileWriteError(const string& operation, const string& filePath,
        size_t expected, size_t written, int errorNumber)
{
    *lout << operation << " short write [file=" << filePath
        << ", expected=" << expected
        << ", written=" << written
        << ", errno=" << errorNumber
        << ", message=" << errnoMessage(errorNumber)
        << "]" << endl;
    *lout << std::flush;
}

static void logFileCloseError(const string& operation, const string& filePath, int errorNumber)
{
    *lout << operation << " close failed [file=" << filePath
        << ", errno=" << errorNumber
        << ", message=" << errnoMessage(errorNumber)
        << "]" << endl;
    *lout << std::flush;
}

FileSystemCompactStorage::FileSystemCompactStorage(const string& dir, int groups)
        : groups(groups), dir(dir)
{
    indices = vector<int>(groups, -1);
    offsets = vector<int>(groups, DATA_FILE_SIZE);
    mutexes.resize(groups);

    readIndexFile();

    *lout << "Initialized FileSystemCompactStorage{dir=" << dir << ", groups=" << groups << "}" << endl;
}

static bool isErased(const Position& position)
{
    return position.group == 0 && position.index == 0 && position.offset == 0
        && position.length == 0 && position.fingerprint == 1;
}

bool FileSystemCompactStorage::has(const string& name)
{
    boost::shared_lock<boost::shared_mutex> scoped_lock(mutex);

    auto it = positionByName.find(name);
    return it != positionByName.end() && !isErased(it->second);
}

void FileSystemCompactStorage::erase(const string& name)
{
    Position position;

    {
        // Only lock until positionByName is updated
        boost::unique_lock<boost::shared_mutex> scoped_lock(mutex);

        auto it = positionByName.find(name);
        if (it == positionByName.end() || isErased(it->second))
            return;

        position = {0, 0, 0, 0, 1};  // Mark as erased
        positionByName[name] = position;
    }

    appendNameAndPosition(name, position);  // Perform I/O after unlocking
}

bool FileSystemCompactStorage::get(const string& name, string& data)
{
    data.clear();
    Position position = {0, 0, 0, 0, 1};
    bool result = false;

    {
        boost::shared_lock<boost::shared_mutex> scoped_lock(mutex); // Read lock
        auto it = positionByName.find(name);
        if (it != positionByName.end())
            position = it->second;
    }

    if (isErased(position))
        return false;

    char groupName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(groupName, "%d", position.group);
    char fileName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(fileName, DATA_FILE_PATTERN.c_str(), position.index);
    char* bytes = 0;

    {
        boost::unique_lock<boost::shared_mutex> scoped_lock(mutexes[position.group]);
        auto filePath = concatPath(dir, concatPath(groupName, fileName));
        FILE* f = fopen(filePath.c_str(), "rb");
        if (0 != f)
        {
            if (0 == fseek(f, position.offset, SEEK_SET))
            {
                bytes = new char[position.length + SIZEOF_INT];
                result = (position.length + SIZEOF_INT == int(fread(bytes, 1, position.length + SIZEOF_INT, f)));
                if (!result)
                {
                    printf("Broken fread\n");
                    lout->fatal("Can't fread file '" + filePath + "'");
                }
            }
            else
                printf("Can't seek\n");
            fclose(f);
        }
        else
        {
            printf("f == 0 [filePath=%s]\n", filePath.c_str());
            lout->fatal("Can't fopen file '" + filePath + "'");
        }
    }

    if (result)
    {
        int fp;
        memcpy(&fp, bytes + position.length, SIZEOF_INT);
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
    
    string filePath = concatPath(dir, concatPath(groupName, fileName));
    errno = 0;
    FILE* f = fopen(filePath.c_str(), "wb");
    if (0 != f)
    {
        errno = 0;
        if (fclose(f) != 0)
            logFileCloseError("prepareDataFile", filePath, errno);
    }
    else
        logFileOpenError("prepareDataFile", filePath, errno);
}

void FileSystemCompactStorage::put(int group, int index, const string& data, int fp)
{
    char groupName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(groupName, "%d", group);
    char fileName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(fileName, DATA_FILE_PATTERN.c_str(), index);
    
    string filePath = concatPath(dir, concatPath(groupName, fileName));
    errno = 0;
    FILE* f = fopen(filePath.c_str(), "ab");
    if (0 != f)
    {
        errno = 0;
        size_t dataWritten = fwrite(data.c_str(), 1, data.length(), f);
        if (dataWritten != data.length())
            logFileWriteError("put data", filePath, data.length(), dataWritten, errno);

        errno = 0;
        size_t fingerprintWritten = fwrite(&fp, 1, SIZEOF_INT, f);
        if (fingerprintWritten != size_t(SIZEOF_INT))
            logFileWriteError("put fingerprint", filePath, size_t(SIZEOF_INT), fingerprintWritten, errno);

        errno = 0;
        if (fclose(f) != 0)
            logFileCloseError("put", filePath, errno);
    }
    else
        logFileOpenError("put", filePath, errno);
}

void FileSystemCompactStorage::put(const string& name, const string& data)
{
    int group = getGroupByName(name, groups);
    int fp = fingerprint(data.c_str(), int(data.length()));

    {
        boost::unique_lock<boost::shared_mutex> global_lock(mutex);
        boost::unique_lock<boost::shared_mutex> group_lock(mutexes[group]);
        
        if (offsets[group] + int(data.length() + SIZEOF_INT) >= DATA_FILE_SIZE)
        {
            indices[group]++;
            offsets[group] = 0;
            prepareDataFile(group, indices[group]);
        }
        
        Position position = {group, indices[group], offsets[group], int(data.length()), fp};
        put(group, indices[group], data, fp);
        
        positionByName[name] = position;
        appendNameAndPosition(name, position);
        
        offsets[group] += int(data.length()) + SIZEOF_INT;
    }
}

void FileSystemCompactStorage::appendNameAndPosition(const string& name, const Position& position)
{
    string indexFile = concatPath(dir, INDEX_FILE);
    errno = 0;
    FILE* indexFilePtr = fopen(indexFile.c_str(), "a+b");
    if (0 != indexFilePtr)
    {
        int length = int(name.length());
        int size = SIZEOF_INT + length + int(sizeof(Position));
        char* data = new char[size];
        memcpy(data, &length, SIZEOF_INT);
        memcpy(data + SIZEOF_INT, name.c_str(), name.length());
        memcpy(data + SIZEOF_INT + length, &position, sizeof(Position));
        errno = 0;
        size_t written = fwrite(data, 1, size_t(size), indexFilePtr);
        if (written != size_t(size))
            logFileWriteError("appendNameAndPosition", indexFile, size_t(size), written, errno);
        delete[] data;

        errno = 0;
        if (fclose(indexFilePtr) != 0)
            logFileCloseError("appendNameAndPosition", indexFile, errno);
    }
    else
        logFileOpenError("appendNameAndPosition", indexFile, errno);
}

void FileSystemCompactStorage::readIndexFile()
{
    auto startTime = std::chrono::steady_clock::now();
    boost::unique_lock<boost::shared_mutex> scoped_lock(mutex);

    // Lock all group-specific mutexes to ensure no race conditions on group data
    std::vector<boost::unique_lock<boost::shared_mutex>> group_locks;
    for (auto& group_mutex : mutexes)
    {
        group_locks.emplace_back(group_mutex);  // Lock each group's mutex
    }

    string indexFile = concatPath(dir, INDEX_FILE);
    boost::system::error_code fileStatusError;
    uintmax_t indexFileSize = 0;
    bool indexFileExists = boost::filesystem::exists(indexFile, fileStatusError);
    if (!indexFileExists && fileStatusError.value() == ENOENT)
        fileStatusError.clear();
    if (indexFileExists && !fileStatusError)
    {
        boost::system::error_code fileSizeError;
        uintmax_t detectedIndexFileSize = boost::filesystem::file_size(indexFile, fileSizeError);
        if (fileSizeError)
            fileStatusError = fileSizeError;
        else
            indexFileSize = detectedIndexFileSize;
    }

    FILE* indexFilePtr = fopen(indexFile.c_str(), "rb");
    
    bool hasError = false;
    bool hasEof = false;
    string indexData;
    
    if (0 != indexFilePtr)
    {
        char block[BLOCK_SIZE];
        
        while (true)
        {
            int read = int(fread(block, 1, BLOCK_SIZE, indexFilePtr));
            
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
            memcpy(&nameLength, indexData.c_str() + pos, SIZEOF_INT);
            pos += SIZEOF_INT;
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
                offsets[position.group] = position.offset + position.length + SIZEOF_INT;
            }

            if (position.index == indices[position.group])
                offsets[position.group] = max(offsets[position.group], position.offset + position.length + SIZEOF_INT);
        }

        fclose(indexFilePtr);
    }

    long long elapsedMillis = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - startTime).count();

    *lout << "Read index file [count=" << positionByName.size()
        << ", bytes=" << indexFileSize
        << ", elapsedMillis=" << elapsedMillis
        << ", exists=" << indexFileExists
        << ", fileStatusError=" << (fileStatusError ? fileStatusError.message() : "none")
        << ", readError=" << hasError
        << ", eof=" << hasEof
        << "]" << endl;
    for (int group = 0; group < groups; group++)
        *lout << "Compact storage group [group=" << group
            << ", index=" << indices[group]
            << ", offset=" << offsets[group]
            << "]" << endl;
}   
