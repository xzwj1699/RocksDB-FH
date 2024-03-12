#pragma once

//#include "core/workload_proxy.h"
//#include "core/core_workload.h"

#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

#include "thread"
#include "sys/mman.h"
#include "sys/syscall.h"
#include "unistd.h"

#include "workloadwrapper.h"

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

static void *AllocBuffer(size_t capacity){
    void *buffer = mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    assert(buffer != MAP_FAILED);
    return buffer;
}

static void DeallocBuffer(void *buffer, size_t capacity){
    assert(munmap(buffer, capacity) == 0);
}


class RocksDBClient3{
	private:
		WorkloadProxy *workload_proxy_;
		rocksdb::DB* db_;
		const rocksdb::Options options_;
		const rocksdb::WriteOptions write_options_;
		const rocksdb::ReadOptions read_options_;

		const std::string data_dir_;

		const int loader_threads_;
		const uint64_t load_num_;
		const int worker_threads_;
		const uint64_t request_num_;
		const int async_num_;
		const double warmup_rate_ = 0.3;

		std::atomic<int> max_pending_req_;

		WorkloadWrapper *workload_wrapper_ = nullptr;

		const uint64_t record_interval_ = 1000000;
		const uint64_t high_interval_ = 1 * 1000000;
		const uint64_t low_interval_ = 20 * 1000000;

		double wal_time_;
		double wait_time_;
		double complete_memtable_time_;
		double block_read_time_;
		double write_delay_time_;
		double write_memtable_time_;
		std::mutex mutex_;
		uint64_t submit_time_;
		std::atomic<bool> is_high_;
		std::atomic<uint64_t> total_finished_requests_;


		struct TimeRecord{
		  private:
			double *data_;
			const uint64_t capacity_;
			uint64_t size_;
			bool sorted;
		  public:
			TimeRecord(uint64_t capacity):
				data_(nullptr),
				capacity_(capacity),
				size_(0),
				sorted(false){
					data_ = (double *)AllocBuffer(capacity_ * sizeof(double));
					assert(data_ != nullptr);
			}
			~TimeRecord(){
				assert(size_ <= capacity_);
				if(data_ != nullptr)
					DeallocBuffer(data_, capacity_ * sizeof(double));
			}
			void Insert(double time){
				data_[size_++] = time;
				assert(size_ <= capacity_);
				sorted = false;
			}
			double *Data(){
				return data_;
			}
			void Join(TimeRecord* time){
				assert(data_ != nullptr);
				assert(time->Data() != nullptr);
				uint64_t pos = __sync_fetch_and_add(&size_, time->Size());
				assert(size_ <= capacity_);
				memcpy(data_ + pos, time->Data(), sizeof(double) * time->Size());
				sorted = false;
			}
			double Sum(){
				assert(data_ != nullptr);
				return std::accumulate(data_, data_ + size_, 0.0);
			}
			uint64_t Size(){
				return size_;
			}
			double Tail(double f){
				assert(data_ != nullptr);
				if(!sorted){
					std::sort(data_, data_ + size_);
					sorted = true;
				}
				return data_[(uint64_t)(size_ * f)];
			}
			void SavetoFile(std::string filename){
				assert(!sorted);
				FILE *out = fopen(filename.c_str(), "w");
		    	for(uint64_t i=0; i<size_; i++){
		     		fprintf(out, "%lf\n",data_[i]);
		     	}
		    	fclose(out);
			}
		};

		TimeRecord *request_time_  = nullptr;
		TimeRecord *read_time_  = nullptr;
		TimeRecord *update_time_ = nullptr;
		TimeRecord *iops_ = nullptr;
		

	public:
		RocksDBClient3(WorkloadProxy *workload_proxy, rocksdb::Options options, rocksdb::WriteOptions write_options,
					  rocksdb::ReadOptions read_options, std::string data_dir, int loader_threads,
					  uint64_t load_num, int worker_threads, uint64_t request_num, int async_num):
				workload_proxy_(workload_proxy),
				options_(options),
				write_options_(write_options),
				read_options_(read_options),
				data_dir_(data_dir),
				loader_threads_(loader_threads),
				load_num_(load_num),
				worker_threads_(worker_threads),
				request_num_(request_num),
				async_num_(async_num),
				max_pending_req_(async_num),
				is_high_(true),
				total_finished_requests_(0){

			rocksdb::Status s = rocksdb::DB::Open(options_, data_dir_, &db_);
			if(!s.ok()){
				printf("%s\n", s.ToString().c_str());
				exit(0);
			}
			Reset();
		}

		~RocksDBClient3(){
			if(db_ != nullptr)
				delete db_;
			if(request_time_ != nullptr)
				delete request_time_;
			if(read_time_ != nullptr)
				delete read_time_;
			if(update_time_ != nullptr)
				delete update_time_;
		}

		void Load();
		void Work();
		void Warmup();

	private:
		inline void SetAffinity(int coreid){
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

		void RocksDBWorker(uint64_t num, int coreid, bool is_warmup, bool is_master);
		void LogsDBWorker(uint64_t num, int coreid, bool is_warmup, bool is_master);
		void RocksdDBLoader(uint64_t num, int coreid);
		void LogsDBLoader(uint64_t num, int coreid);

		void AsyncLoadThread(uint64_t num, int coreid);

		void Reset();

		void PrintArgs();
};


}