FROM alpine:3.18.4

RUN apk update && apk add --no-cache bash \
bash-doc                                  \
bash-completion                           \
build-base                                \
git                                       \
linux-headers                             \
make                                      \
cmake                                     \
zlib-dev                                  \
bzip2-dev                                 \
lz4-dev                                   \
gflags-dev                                \
snappy-dev                                \
zstd-dev                                  \
g++                                       \
boost-system                              \
boost-thread                              \
boost-filesystem                          \
boost-program_options                     \
leveldb-dev                               \
rocksdb-dev

RUN apk update && apk add --no-cache boost-dev

RUN apk update && apk add --no-cache valgrind

RUN mkdir /src
COPY *.cpp *.h /src/
RUN cd /src && g++ -std=c++20 -Wall -Wextra -Wconversion -Ofast -o riorita riorita.cpp protocol.cpp compact.cpp storage.cpp cache.cpp -lboost_system -lboost_thread -lboost_filesystem -lboost_program_options -lpthread -lleveldb -lsnappy -lrocksdb

RUN mkdir /riorita
WORKDIR /riorita

COPY riorita.sh /
EXPOSE 8100

ENTRYPOINT ["/riorita.sh"]
