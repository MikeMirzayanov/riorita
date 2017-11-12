call "C:\Program Files (x86)\Microsoft Visual Studio\2017\Enterprise\VC\Auxiliary\Build\vcvars64.bat" 
set LD=N:\Libs\boost_1_65_1\stage\x64\lib\
cl.exe /Feriorita_engine.dll /O2 /MT /EHsc /IC:\Programs\Java-8-64\include /IC:\Programs\Java-8-64\include\win32 /IN:\Libs\boost_1_65_1 /LD compact.cpp riorita_engine.cpp /link %LD%libboost_thread-vc141-mt-s-1_65_1.lib %LD%libboost_system-vc141-mt-s-1_65_1.lib %LD%libboost_date_time-vc141-mt-s-1_65_1.lib %LD%libboost_chrono-vc141-mt-s-1_65_1.lib %LD%libboost_filesystem-vc141-mt-s-1_65_1.lib
xcopy /Y riorita_engine.dll ..\src\main\files\
