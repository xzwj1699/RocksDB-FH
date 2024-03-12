cd ..
./a.sh
cd ycsb

[[ -d build ]] && rm -rf build
mkdir build && cd build
cmake ../
make
cd ..
