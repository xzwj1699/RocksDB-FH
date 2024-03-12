[[ -d build ]] && rm -rf build
mkdir build && cd build
cmake ../
#-DCMAKE_CXX_COMPILER=/home/v-ziyueqiu/RocksDB-SSDLogging/gcc-7.3.0/src/bin/g++
make
cd ..
