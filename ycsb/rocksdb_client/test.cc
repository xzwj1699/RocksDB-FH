/*
* 2020-09-02
*/

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/table.h"

#include "thread"
#include "mutex"
#include "unistd.h"
#include "set"

#include "sys/mman.h"
#include "sys/syscall.h"
#include "unistd.h"

#include "random"
#include "time.h"
#include "atomic"

#if defined(__GNUC__) && __GNUC__ >= 4
	#define LIKELY(x)	 (__builtin_expect((x), 1))
	#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
	#define LIKELY(x)   (x)
	#define UNLIKELY(x) (x)
#endif

using namespace rocksdb;

#define NOW (std::chrono::high_resolution_clock::now())
#define DURATION(start, end) (std::chrono::duration<double>((end)-(start)).count() * 1000 * 1000)
typedef std::chrono::high_resolution_clock::time_point TimePoint;
uint64_t key_index = 0;

inline void SetAffinity(int coreid){
  int cores_num = sysconf(_SC_NPROCESSORS_ONLN);
  coreid = coreid % cores_num;
  if(coreid == 0)
    coreid = 0;
  else
    coreid = cores_num - coreid;
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(coreid, &mask);
  printf("client core: %d\n", coreid);
	int rc = sched_setaffinity(syscall(__NR_gettid), sizeof(mask), &mask);
	assert(rc == 0);
}

inline void SetAffinitySPDK(int coreid){
			coreid = coreid % sysconf(_SC_NPROCESSORS_ONLN);
			assert(coreid >= 0);
			if(coreid == 39)
				coreid = 0;
			else if(coreid == 40)
				coreid = 3;
			else if(coreid % 2 == 1)
    			coreid = (coreid+1)/2 * 4;
  			else
    			coreid = coreid * 2 + 3;
			printf("client coreid: %d\n", coreid);
			cpu_set_t mask;
			CPU_ZERO(&mask);
			CPU_SET(coreid, &mask);
			int rc = sched_setaffinity(syscall(__NR_gettid), sizeof(mask), &mask);
			assert(rc == 0);
}

const uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325;
const uint64_t kFNVPrime64 = 1099511628211;
inline uint64_t FNVHash64(uint64_t val) {
  uint64_t hash = kFNVOffsetBasis64;
  for (int i = 0; i < 8; i++) {
    uint64_t octet = val & 0x00ff;
    val = val >> 8;
    hash = hash ^ octet;
    hash = hash * kFNVPrime64;
  }
  return hash;
}

void normal_async_send_req(DB *db, WriteOptions write_options, int num, int coreid, double *time, int index, int async_num);

int main(){
     // open DB
    std::string kDBPath = "/home/spdk/sata/rocksdb/data";

	rocksdb::Options options;
	rocksdb::WriteOptions write_options;
	rocksdb::ReadOptions read_options;
	options.allow_concurrent_memtable_write = true;
	options.recycle_log_file_num = false;
	options.allow_2pc = false;
	options.compression = rocksdb::kNoCompression;
	options.max_open_files = 50000;
	write_options.sync = true;
	write_options.disableWAL = false;
  	options.wal_dir = kDBPath;
	options.error_if_exists = true;
	options.create_if_missing = true;
	options.statistics = rocksdb::CreateDBStatistics();
	options.max_total_wal_size = 2<<30 ;//256ull<<20;//(1ull << 30);
	options.write_buffer_size = 2<<30;//256ull<<20;

	auto env = rocksdb::Env::Default();
	env->SetBgThreadCores(1, rocksdb::Env::HIGH);
	env->SetBgThreadCores(3, rocksdb::Env::LOW);
	env->SetBackgroundThreads(1, rocksdb::Env::HIGH);
	env->SetBackgroundThreads(3, rocksdb::Env::LOW);
	options.max_background_jobs = 4;
	options.max_write_buffer_number = 4;
	options.auto_config = false;
  
	options.env = env;
	options.enable_spdklogging = true;
	options.ssdlogging_type = "spdk";
	options.spdk_recovery = true;
	options.wal_dir = kDBPath;
	options.ssdlogging_path = "trtype:PCIe traddr:0000:c9:00.0";
	options.ssdlogging_num = 6;
	options.logging_server_num = 1;
	options.spandb_compactor_num = 0;
	options.spandb_worker_num = 20;
	options.max_read_que_length = 2;
	options.auto_config = false;
	options.dynamic_moving = false;
	options.max_subcompactions = 16;
	DB* db;
	if(!options.spdk_recovery){
		system(("rm -r " + kDBPath + "*").c_str());
	}else{
		options.error_if_exists = false;
	}
	Status s = DB::Open(options, kDBPath, &db);
	if(!s.ok()){
	   printf("%s\n", s.ToString().c_str());
	   exit(0);
	}

	if(options.spdk_recovery){
		return 0;
	}

	int n = 1;
	int reqs = 1000000;
	int cores = sysconf(_SC_NPROCESSORS_ONLN);
	double *time = new double[n];
	auto start = NOW;
	std::vector<std::thread> threads;
	printf("start\n");
	for(int i=0; i<n; i++){
		threads.emplace_back(normal_async_send_req, db, write_options, reqs, 30 + i, time, i, 10);
	}
	for(int i=0;i<n;i++){
		threads[i].join();
	}
	printf("thpt: %.2lf\n", (n * reqs) / (DURATION(start, NOW) / 1000000.0));
	printf("Stall: %.6lf s\n", options.statistics->getTickerCount(rocksdb::STALL_MICROS)/1000000.0);
	std::string stat_str2;
	db->GetProperty("rocksdb.stats", &stat_str2);
	printf("\n%s\n", stat_str2.c_str());
	  // db->GetProperty("rocksdb.stats", &stat_str2);
	 	// printf("\n%s\n", stat_str2.c_str());
	delete db;
}

void normal_async_send_req(DB *db, WriteOptions write_options, int num, int coreid, double *time, int index, int async_num){
    SetAffinitySPDK(coreid);
    std::string value(1024, 'a');
    //1. prepare pipeline
    TimePoint *senttime = new TimePoint[async_num];
    std::atomic<Status*> *status = new std::atomic<Status*>[async_num];
    bool *occupied = new bool[async_num];
    for(int i=0; i<async_num; i++){
        status[i].store(nullptr);
        occupied[i] = false;
    }
    int k=0;
    //3. start
    double t = 0;
    // auto start = NOW;
    while(true){
        //send a request
        for(int i=0; i<async_num; i++){
            if(!occupied[i]){
                if(LIKELY(k < num)){
                    if(k%1000000 == 0)
                        printf("%d send %d requests\n", index, k);
                    occupied[i] = true;
                    //std::string key = "test-" + std::to_string(__sync_fetch_and_add(&key_index, 1));
                    std::string key = "test-" + std::to_string(FNVHash64(__sync_fetch_and_add(&key_index, 1)));
                    //std::string key = "rocksdb_test_" + std::to_string(std::rand() * std::rand() * std::rand() * std::rand() * std::rand());
                    assert( status[i] == nullptr);
                    senttime[i] = NOW;
                    // printf("%s\n", key.c_str());
                    Status s = db->AsyncPut(write_options, key, value, status[i]);
                    // Status s = db->AsyncPut(write_options, key, key, status[i]);
                    if(!s.ok()){
                        occupied[i] = false;
                        break;
                    }
                    k++;
                }
                break;
            }
        }
        //check completion
        bool finished = true;
        for(int i=0; i<async_num; i++){
            if(occupied[i]){
                if(UNLIKELY(status[i].load() != nullptr)){
                    if(!status[i].load()->ok()){ 
                        fprintf(stderr,"%s: %d: %s\n", __FILE__, __LINE__, status[i].load()->ToString().c_str());
                        assert(false);
                    }
                    delete status[i].load();
                    status[i].store(nullptr);
                    occupied[i] = false;
                    t += DURATION(senttime[i], NOW);
                }
                finished = false;
            }
        }
        //finish all requests
        if(UNLIKELY(finished))
            break;
    }
    time[index] = t;
}
