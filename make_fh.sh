make rocksdbjava -j16
cp java/target/rocksdbjni-8.3.2-linux64.jar YCSB/rocksdb/target/dependency/rocksdbjni-8.3.2-fh.jar
cd YCSB
mvn -pl site.ycsb:rocksdb-binding -am package -e