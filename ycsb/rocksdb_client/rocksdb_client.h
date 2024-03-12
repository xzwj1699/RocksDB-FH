#pragma once

#include "core/workload_proxy.h"
#include "core/core_workload.h"

#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/merge_operator.h"


#include "thread"
#include "sys/mman.h"
#include "sys/syscall.h"
#include "unistd.h"

namespace ycsbc{

#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION(start, end) (std::chrono::duration<double>((end)-(start)).count() * 1000 * 1000)
typedef std::chrono::high_resolution_clock::time_point TimePoint;

#if defined(__GNUC__) && __GNUC__ >= 4
	#define LIKELY(x)	 (__builtin_expect((x), 1))
	#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
	#define LIKELY(x)   (x)
	#define UNLIKELY(x) (x)
#endif

#define ERR(expr) do{rocksdb::Status ec = (expr); if(!ec.ok()) fprintf(stderr,  \
    "%s:%d: %s: %s\n", __FILE__, __LINE__, #expr, ec.ToString().c_str()), assert(false);}while(0)


#define CAPACITY (1ul << 30)

inline void *AllocBuffer(size_t capacity){
    void *buffer = mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    assert(buffer != MAP_FAILED);
    return buffer;
}

inline void DeallocBuffer(void *buffer, size_t capacity){
    assert(munmap(buffer, capacity) == 0);
}

class RocksDBClient{
	private:
		WorkloadProxy *workload_proxy_;

		rocksdb::TransactionDB* txn_db;
		rocksdb::Options options_;
		rocksdb::WriteOptions write_options_;
		rocksdb::TransactionDBOptions txndb_options_;
		rocksdb::ReadOptions read_options_;
		rocksdb::TransactionOptions txn_op_;

		std::string data_dir_;

		int loader_threads_;
		uint64_t load_num_;
		int worker_threads_;
		uint64_t request_num_;

		double *request_time_;
		uint64_t request_time_num_;

		double wal_time_;
		double wait_time_;
		double block_read_time_;
		double total_time_;
		double update_time_;
		double read_time_;
		uint64_t update_num_;
		uint64_t read_num_;
		std::mutex mutex_;

		int async_num_ = 4;

		void Reset();
		void PrintArgs();

	public:
		RocksDBClient(WorkloadProxy *workload_proxy, rocksdb::Options options, rocksdb::WriteOptions write_options,
					  rocksdb::TransactionDBOptions txndb_options, std::string data_dir, int loader_threads,
					  uint64_t load_num, int worker_threads, uint64_t request_num):
				workload_proxy_(workload_proxy),
				options_(options),
				write_options_(write_options),
				txndb_options_(txndb_options),
				read_options_(),
				txn_op_(),
				data_dir_(data_dir),
				loader_threads_(loader_threads),
				load_num_(load_num),
				worker_threads_(worker_threads),
				request_num_(request_num){

			/*rocksdb::BlockBasedTableOptions table_options;
			table_options.block_cache = rocksdb::NewLRUCache((1ul << 30) * 8);
			options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));*/
			//options_.table_cache_numshardbits = 10;

			options_.statistics = rocksdb::CreateDBStatistics();

			rocksdb::Status s = rocksdb::TransactionDB::Open(options_, txndb_options_, data_dir_, &txn_db);
			if(!s.ok()){
				printf("%s\n", s.ToString().c_str());
				exit(0);
			}
			Reset();


		}

		~RocksDBClient(){
			if(txn_db != nullptr)
				delete txn_db;
		}

		void Load();

		void Work();

	private:
		inline void SetAffinity(int coreid){
			cpu_set_t mask;
			CPU_ZERO(&mask);
			CPU_SET(coreid, &mask);
			int rc = sched_setaffinity(syscall(__NR_gettid), sizeof(mask), &mask);
			assert(rc == 0);
		}

		void LoadThread(uint64_t num, int coreid);

		void AsyncLoadThread(uint64_t num, int coreid);

		void WorkThread(uint64_t num, int coreid);

		void AsyncWorkThread(uint64_t num, int coreid);

		void Read();

		void ReadModifyWrite(rocksdb::Transaction *txn);

		void Scan();

		void Update(rocksdb::Transaction *txn);

		void Insert(rocksdb::Transaction *txn);
};

}