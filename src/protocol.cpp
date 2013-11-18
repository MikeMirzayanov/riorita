#include "protocol.h"

#include <cstring>
#include <cassert>
//#include <iostream>

using namespace riorita;
using namespace std;

namespace riorita {

byte toByte(RequestType requestType) {
    return byte(requestType);
}

Bytes::Bytes(int32 size, byte* data): size(size), data(data) {
    // No operations.
}

void Bytes::reset()
{
    if (null != data)
    {
        for (int i = 0; i < size; i++)
            data[i] = 0;
        delete[] data;
        data = null;
        size = 0;
    }
}

Request* parseRequest(Bytes& bytes, int32 pos, int32& parsedByteCount)
{
    parsedByteCount = 0;

    int32 headerSize = sizeof(byte) // magic byte
        + sizeof(byte) // protocol
        + sizeof(byte) // type
        + sizeof(RequestId) // request id
        + sizeof(int32) // key length
    ;
    
    // cout << "headerSize=" << headerSize << endl;

    int32 lengthSize = sizeof(int32);
    
    if (pos + headerSize <= bytes.size)
    {
        if (bytes.data[pos++] != MAGIC_BYTE)
            return null;
        parsedByteCount++;
        //cout << "MAGIC_BYTE found" << endl;
    
        if (bytes.data[pos++] != PROTOCOL_VERSION)
            return null;
        parsedByteCount++;
        //cout << "PROTOCOL_VERSION found" << endl;

        byte typeByte = bytes.data[pos++];
        if (typeByte < PING || typeByte > DELETE)
            return null;
        parsedByteCount++;
        //cout << "type=" << typeByte << endl;
        RequestType type = RequestType(typeByte);

        RequestId id;
        memcpy(&id, bytes.data + pos, sizeof(RequestId));
        pos += sizeof(RequestId);
        parsedByteCount += sizeof(RequestId);
        //cout << "id=" << id << endl;

        int32 keyLength;
        memcpy(&keyLength, bytes.data + pos, lengthSize);
        pos += lengthSize;
        if (keyLength < 0)
            return null;
        parsedByteCount += lengthSize;

        if (pos + keyLength > bytes.size)
            return null;

        Bytes key(keyLength, bytes.data + pos);
        pos += keyLength;
        parsedByteCount += keyLength;

        Bytes value;
        if (type == PUT)
        {
            //cout << "put " << pos << " " << bytes.size << endl;

            if (pos + lengthSize > bytes.size)
                return null;

            //cout << "put" << endl;

            int32 valueLength;
            memcpy(&valueLength, bytes.data + pos, lengthSize);
            pos += lengthSize;
            //cout << "put" << endl;

            if (valueLength < 0)
                return null;
            //cout << "put" << endl;

            parsedByteCount += lengthSize;

            //cout << "valueLength=" << valueLength << endl;

            if (pos + valueLength > bytes.size)
                return null;

            value = Bytes(valueLength, bytes.data + pos);
            pos += valueLength;
            parsedByteCount += valueLength;
        }

        return new Request(type, id, key, value);
    }
    else
        return null;
}

inline int32 copyBytes(int32 valueSize, const byte* value, byte* data)
{
    memcpy(data, value, valueSize);
    return valueSize;
}

inline int32 copyByte(byte value, byte* data)
{
    memcpy(data, &value, sizeof(byte));
    return sizeof(byte);
}

inline int32 copyInt32(int32 value, byte* data)
{
    memcpy(data, &value, sizeof(int32));
    return sizeof(int32);
}

inline int32 copyInt64(int64 value, byte* data)
{
    memcpy(data, &value, sizeof(int64));
    return sizeof(int64);
}

inline int32 copyRequestHeader(const Request& request, bool success, byte* data)
{
    int32 pos = 0;

    pos += copyByte(MAGIC_BYTE, data + pos);
    pos += copyByte(PROTOCOL_VERSION, data + pos);
    pos += copyInt64(request.id, data + pos);
    pos += copyByte(success ? 1 : 0, data + pos);

    return pos;
}

Bytes newResponse(const Request& request, bool success, bool verdict, int32 dataSize, const byte* data)
{
    int32 headerSize = sizeof(int32) // total size
        + sizeof(byte) // magic byte
        + sizeof(byte) // protocol
        + sizeof(RequestId) // request id
        + sizeof(byte) // success
    ;

    int32 byteCount = headerSize + (success
        ? 1 + (request.type == GET && verdict
            ? sizeof(int32) + dataSize
            : 0
        )
        : 0
    );

    byte* result = new byte[byteCount];
    
    int32 pos = copyInt32(byteCount, result);
    pos += copyRequestHeader(request, success, result + pos);

    if (success)
    {
        pos += copyByte(verdict ? 1 : 0, result + pos);
        if (request.type == GET && verdict)
        {
            pos += copyInt32(dataSize, result + pos);
            pos += copyBytes(dataSize, data, result + pos);
        }
    }

    assert(byteCount == pos);
    return Bytes(byteCount, result);
}

}
