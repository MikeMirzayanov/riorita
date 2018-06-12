mv -f /usr/lib/gcc/x86_64-redhat-linux/7/crtbeginT.o /usr/lib/gcc/x86_64-redhat-linux/7/crtbeginT.o.tmp
cp -f /usr/lib/gcc/x86_64-redhat-linux/7/crtbeginS.o /usr/lib/gcc/x86_64-redhat-linux/7/crtbeginT.o
BOOST=/root/boost_1_65_1/boost_output
g++ -D_GLIBCXX_USE_CXX11_ABI=0 -std=c++14 -fPIC -O3 -static -static-libgcc -static-libstdc++ -shared -o riorita_engine.so -I /usr/lib/jvm/java-1.8.0/include -I /usr/lib/jvm/java-1.8.0/include/linux -I $BOOST/include compact.cpp riorita_engine.cpp $BOOST/lib/libboost_system.a $BOOST/lib/libboost_thread.a $BOOST/lib/libboost_filesystem.a -lpthread
mv -f /usr/lib/gcc/x86_64-redhat-linux/7/crtbeginT.o.tmp /usr/lib/gcc/x86_64-redhat-linux/7/crtbeginT.o
cp -f riorita_engine.so ../src/main/files/riorita_engine.so
