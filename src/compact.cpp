#include "compact.h"

#include <cstdio>
#include <cstring>
#include <cassert>

using namespace riorita;
using namespace std;

const string INDEX_FILE = "FileSystemCompactStorage.index";
const string DATA_FILE_PATTERN = "FileSystemCompactStorage.%04d";
const int BLOCK_SIZE = 1024 * 1024;
const int DATA_FILE_SIZE = 1024 * 1024 * 1024;
const int MAX_DATA_FILE_NAME_LENGTH = 64;

static int fingerprint(const char* c, int size)
{
    int result = 0;
    for (int i = 0; i < size; i++)
        result = result * 97 + int(int(c[i]) + 255);
    return result;
}

static string concatPath(const string& dir, const string& child)
{
#if defined(_WIN32) || defined(WIN32)
    return dir + "\\" + child;
#else
    return dir + "/" + child;
#endif
}

FileSystemCompactStorage::FileSystemCompactStorage(const string& dir): dir(dir), index(-1), offset(DATA_FILE_SIZE)
{
    readIndexFile();
}

static bool isErased(const Position& position)
{
    return position.index == 0 && position.offset == 0
        && position.length == 0 && position.fingerprint == 1;
}

bool FileSystemCompactStorage::has(const string& name)
{
    return positionByName.count(name) && !isErased(positionByName[name]);
}

void FileSystemCompactStorage::erase(const string& name)
{
    if (has(name))
    {
        Position position = {0, 0, 0, 1};
        positionByName[name] = position;
        appendNameAndPosition(name, position);
    }
}

bool FileSystemCompactStorage::get(const string& name, string& data)
{
    data.clear();
    bool result = false;

    if (positionByName.count(name))
    {
        Position position = positionByName[name];
        char fileName[MAX_DATA_FILE_NAME_LENGTH];
        sprintf(fileName, DATA_FILE_PATTERN.c_str(), position.index);
        FILE* f = fopen(concatPath(dir, fileName).c_str(), "rb");
        if (0 != f)
        {
            if (0 == fseek(f, position.offset, SEEK_SET))
            {
                char* bytes = new char[position.length + sizeof(int)];
                if (position.length + sizeof(int) == fread(bytes, 1, position.length + sizeof(int), f))
                {
                    int fp;
                    memcpy(&fp, bytes + position.length, sizeof(int));
                    result = (position.fingerprint == fingerprint(bytes, position.length)
                        && position.fingerprint == fp);
                    if (result)
                        data.append(bytes, position.length);
                }
                delete[] bytes; 
            }
            fclose(f);
        }                              
    }                           
    return result;
}

void FileSystemCompactStorage::prepareDataFile(int index)
{
    char fileName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(fileName, DATA_FILE_PATTERN.c_str(), index);
    
    FILE* f = fopen(concatPath(dir, fileName).c_str(), "wb");
    if (0 != f)
        fclose(f);
}

void FileSystemCompactStorage::put(int index, const string& data, int fp)
{
    char fileName[MAX_DATA_FILE_NAME_LENGTH];
    sprintf(fileName, DATA_FILE_PATTERN.c_str(), index);
    
    FILE* f = fopen(concatPath(dir, fileName).c_str(), "ab");
    if (0 != f)
    {
        fwrite(data.c_str(), 1, data.length(), f);
        fwrite(&fp, 1, sizeof(int), f);
        fclose(f);
    }
}

void FileSystemCompactStorage::put(const string& name, const string& data)
{
    if (offset + int(data.length() + sizeof(int)) >= DATA_FILE_SIZE)
    {
        index++;
        offset = 0;
        prepareDataFile(index);
    }

    int fp = fingerprint(data.c_str(), data.length());
    
    put(index, data, fp);
    
    Position position = {index, offset, int(data.length()), fp};
    
    positionByName[name] = position;
    appendNameAndPosition(name, position);

    offset += data.length() + sizeof(int);
}

void FileSystemCompactStorage::appendNameAndPosition(const string& name, const Position& position)
{
    string indexFile = concatPath(dir, INDEX_FILE);
    FILE* indexFilePtr = fopen(indexFile.c_str(), "a+b");
    if (0 != indexFilePtr)
    {
        int size = sizeof(int) + name.length() + sizeof(Position);
        char* data = new char[size];
        int length = name.length();
        memcpy(data, &length, sizeof(int));
        memcpy(data + sizeof(int), name.c_str(), name.length());
        memcpy(data + sizeof(int) + name.length(), &position, sizeof(Position));
        fwrite(data, 1, size, indexFilePtr);
        delete[] data;
    }
    fclose(indexFilePtr);
}

void FileSystemCompactStorage::readIndexFile()
{
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
        
        assert(sizeof(Position) == 16);

        int pos = 0;
        while (pos < int(indexData.length()) && !hasError && hasEof)
        {
            int nameLength;
            memcpy(&nameLength, indexData.c_str() + pos, sizeof(int));
            pos += sizeof(int);
            string name(indexData.c_str() + pos, nameLength);
            pos += nameLength;
            Position position;
            memcpy(&position, indexData.c_str() + pos, sizeof(Position));
            pos += sizeof(Position);

            positionByName[name] = position;

            if (position.index > index)
            {
                index = position.index;
                offset = position.offset + position.length;
            }

            if (position.index == index)
                offset = max(offset, position.offset + position.length);
        }

        fclose(indexFilePtr);
    }
}

/*

#define forn(i, n) for (int i = 0; i < int(n); i++)

string key(int num)
{
    srand(num);
    string c(12, '0');
    for (int i = 0; i < c.length(); i++)
        c[i] = char('0' + rand() % 10);
    return c;
}

string value(int num)
{
    srand(num);
    string c(500000, '0');
    for (int i = 0; i < c.length(); i++)
        c[i] = char('0' + rand() % 10);
    return c;
}

#include <ctime>
#include <vector>
#include <iostream>

int main(int argc, char* argv[])
{
    int t;
    scanf("%d", &t);

    FileSystemCompactStorage s(".");

    int tt = clock();

    vector<string> keys(100);
    forn(i, keys.size())
        keys[i] = key(i);

    if (t == 5)
    {
        forn(i, keys.size())
            s.put(keys[i], value(i));
    }

    if (t == 6)
    {
        srand(16);
        forn(i, keys.size())
        {
            int j = rand() % keys.size();
            string v;
            assert(s.get(keys[j], v));
            assert(v == value(j));
        }
    }

    cout << (clock() - tt) * 1000.0 / CLOCKS_PER_SEC << endl;

    return 0;
}
*/