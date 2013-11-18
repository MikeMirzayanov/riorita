#ifndef RIORITA_PROTOCOL_H_
#define RIORITA_PROTOCOL_H_

#include <cstdlib>

namespace riorita {

typedef unsigned char byte;
typedef unsigned long long RequestId;
typedef int int32;
typedef long long int64;

const byte MAGIC_BYTE = 113;
const byte PROTOCOL_VERSION = 1;

#define null (0)

enum RequestType {
    PING = 1,
    HAS = 2,
    GET = 3,
    PUT = 4,
    DELETE = 5
};

byte toByte(RequestType requestType);

struct Bytes {
    int32 size;
    byte* data;
    Bytes(int32 size = 0, byte* data = null);
    void reset();
};

struct Request {
    Request(RequestType type, RequestId id, Bytes key, Bytes value):
            type(type), id(id), key(key), value(value) {
        // No operations.
    }

    RequestType type;
    RequestId id;
    Bytes key;
    Bytes value;
};

Request* parseRequest(Bytes& bytes, int32 pos, int32& parsedByteCount);

Bytes newResponse(const Request& request, bool success, bool verdict, int32 dataSize, const byte* data);

}

#endif
