mv -f /usr/lib/gcc/x86_64-redhat-linux/7/crtbeginT.o /usr/lib/gcc/x86_64-redhat-linux/7/crtbeginT.o.tmp
cp -f /usr/lib/gcc/x86_64-redhat-linux/7/crtbeginS.o /usr/lib/gcc/x86_64-redhat-linux/7/crtbeginT.o
g++ -std=c++14 -fPIC -O2 -g -static -static-libgcc -static-libstdc++ -shared -o riorita_engine.so -I /usr/lib/jvm/java-1.8.0/include -I /usr/lib/jvm/java-1.8.0/include/linux compact.cpp riorita_engine.cpp -lboost_system -lboost_thread -lboost_filesystem -lboost_program_options -lpthread -lsnappy
mv -f /usr/lib/gcc/x86_64-redhat-linux/7/crtbeginT.o.tmp /usr/lib/gcc/x86_64-redhat-linux/7/crtbeginT.o
cp -f riorita_engine.so ../src/main/files/riorita_engine.so
