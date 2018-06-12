g++ -std=c++14 -Wall -Wextra -Wconversion  -DHAS_ROCKSDB -DHAS_LEVELDB -O2 -g -o riorita riorita.cpp protocol.cpp compact.cpp storage.cpp cache.cpp -lboost_system -lboost_thread -lboost_filesystem -lboost_program_options -lpthread -lleveldb -lsnappy -I../../rocksdb/include -L../../rocksdb -lrocksdb

