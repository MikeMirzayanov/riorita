call "C:\Program Files (x86)\Microsoft Visual Studio\2017\Enterprise\VC\Auxiliary\Build\vcvars64.bat" 
set SNAPPY_HOME=C:\Lib\snappy-windows-1.1.1.8
set BOOST_HOME=C:\Lib\boost_1_67_0
cl.exe /F268435456 /O2 /MT /EHsc /I%SNAPPY_HOME%\include /I%BOOST_HOME% /Feriorita.exe riorita.cpp protocol.cpp compact.cpp storage.cpp cache.cpp /link /LIBPATH:%BOOST_HOME%\lib64-msvc-14.1 libboost_system-vc141-mt-s-x64-1_67.lib libboost_thread-vc141-mt-s-x64-1_67.lib libboost_filesystem-vc141-mt-s-x64-1_67.lib libboost_program_options-vc141-mt-s-x64-1_67.lib snappy.lib
