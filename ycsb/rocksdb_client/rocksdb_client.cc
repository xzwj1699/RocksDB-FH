#include "rocksdb_client.h"
#include "algorithm"

#define GET_TIME_OUTPUT(opt, time) \
do { \
	auto start = TIME_NOW; \
	opt; \
	time =  TIME_DURATION(start, TIME_NOW); \
} while(0);

namespace ycsbc{

void RocksDBClient::Load(){
	Reset();
	int cores_num = sysconf(_SC_NPROCESSORS_ONLN);	
	request_time_ = (double *)AllocBuffer((load_num_ + 1) * sizeof(double));
	request_time_num_ = 0;
	uint64_t num = load_num_ / loader_threads_;
	std::vector<std::thread> threads;

	auto start = TIME_NOW;
	std::function< void(int, int)> fn;
	if(txndb_options_.write_policy == 
			rocksdb::TxnDBWritePolicy::ASYNC_WRITE_PREPARED || 
	   txndb_options_.write_policy == 
			rocksdb::TxnDBWritePolicy::ASYNC_WRITE_COMMITTED){
		fn = std::bind(&RocksDBClient::AsyncLoadThread, this, 
						std::placeholders::_1, std::placeholders::_2);
	}else{
		fn = std::bind(&RocksDBClient::LoadThread, this,
						std::placeholders::_1, std::placeholders::_2);
	}
	for(int i=0; i<loader_threads_; i++){
		if(i == loader_threads_ - 1)
			num = num + load_num_ % loader_threads_;
		threads.emplace_back(fn, num, i%cores_num);
	}
	for(auto &t : threads)
		t.join();
	double time = TIME_DURATION(start, TIME_NOW);

	assert(request_time_num_ == load_num_);
	std::sort(request_time_, request_time_ + load_num_);
	double total_time = std::accumulate(request_time_, request_time_ + load_num_, 0.0);
	double avg = total_time / load_num_;

	printf("---------------------Load----------------------------\n");
	PrintArgs();
	printf("Load %ld requests in %.3lf seconds.\n", load_num_, time/1000/1000);
	printf("Load latency: %.2lf us\n", avg);
	printf("Load IOPS: %.2lf K\n", load_num_/time*1000*1000/1000);
	printf("Load P999: %.2lfus, P99: %.2lfus, P95: %.2lfus, P90: %.2lfus, P75: %.2lfus\n",
			request_time_[(uint64_t)(load_num_*0.999)], request_time_[(uint64_t)(load_num_*0.99)],
			request_time_[(uint64_t)(load_num_*0.95)], request_time_[(uint64_t)(load_num_*0.90)],
			request_time_[(uint64_t)(load_num_*0.75)]);
	printf("WAL sync time: %lf us\n", wal_time_/load_num_);
	printf("Wait time: %lf us\n", wait_time_/load_num_);
	printf("-----------------------------------------------------\n");
	DeallocBuffer(request_time_, (load_num_ + 1) * sizeof(double));
	//std::string stat_str2;
 	//txn_db->GetProperty("rocksdb.stats", &stat_str2);
 	//printf("\n%s\n", stat_str2.c_str());
}

void RocksDBClient::Work(){
	int cores_num = sysconf(_SC_NPROCESSORS_ONLN);
	Reset();
	request_time_ = (double *)AllocBuffer((request_num_ + 1) * sizeof(double));
	request_time_num_ = 0;

	// std::string stat_str1;
	// txn_db->GetProperty("rocksdb.stats", &stat_str1);
	// printf("\n%s\n", stat_str1.c_str());

	int num = request_num_ / worker_threads_;
	std::vector<std::thread> threads;
	auto start = TIME_NOW;
	std::function< void(int, int)> fn;
	if(txndb_options_.write_policy == 
			rocksdb::TxnDBWritePolicy::ASYNC_WRITE_PREPARED || 
	   txndb_options_.write_policy == 
			rocksdb::TxnDBWritePolicy::ASYNC_WRITE_COMMITTED){
		fn = std::bind(&RocksDBClient::AsyncWorkThread, this, 
						std::placeholders::_1, std::placeholders::_2);
	}else{
		fn = std::bind(&RocksDBClient::WorkThread, this, 
						std::placeholders::_1, std::placeholders::_2);
	}
	for(int i=0; i<worker_threads_; i++){
		if(i == worker_threads_ - 1)
			num = num + request_num_ % worker_threads_;
		threads.emplace_back(fn, num, i%cores_num);
	}
	for(auto &t : threads)
		t.join();
	double t = TIME_DURATION(start, TIME_NOW);

	assert(request_time_num_ == request_num_);
	std::sort(request_time_, request_time_ + request_num_);

	double last  = 0;
	for(uint64_t i=0; i<request_num_; i++){
		assert(last <= request_time_[i]);
		last = request_time_[i];
	}

	double total_time = std::accumulate(request_time_, request_time_ + request_num_, 0.0);
	double avg = total_time / request_num_;
	printf("---------------------Request----------------------------\n");
	PrintArgs();
	printf("Finish %ld requests in %.3lf seconds.\n", request_num_, t/1000/1000);
	if(read_num_ != 0){
		printf("read: %ld, read latency: %lf\n", read_num_, read_time_/read_num_);
	}
	if(update_num_ != 0){
		printf("update: %ld, update latency: %lf\n", update_num_, update_time_/update_num_);
	}
	printf("Work latency: %.2lf us\n", avg);
	printf("Work IOPS: %.2lf K\n", request_num_/t*1000*1000/1000);
	printf("Work P999: %.2lfus, P99: %.2lfus, P95: %.2lfus, P90: %.2lfus, P75: %.2lfus\n",
			request_time_[(uint64_t)(request_num_*0.999)], request_time_[(uint64_t)(request_num_*0.99)],
			request_time_[(uint64_t)(request_num_*0.95)], request_time_[(uint64_t)(request_num_*0.90)],
			request_time_[(uint64_t)(request_num_*0.75)]);
	printf("WAL sync time: %lf\n", wal_time_/update_time_);
	printf("Wait time: %lf\n", wait_time_/update_time_);
	printf("Block read time: %lf\n", block_read_time_/read_num_);
	printf("block cache miss: %ld\n", options_.statistics->getTickerCount(rocksdb::BLOCK_CACHE_MISS));
	printf("block cache hit: %ld\n", options_.statistics->getTickerCount(rocksdb::BLOCK_CACHE_HIT));
	printf("memtable hit: %ld\n", options_.statistics->getTickerCount(rocksdb::MEMTABLE_HIT));
	printf("memtable miss: %ld\n", options_.statistics->getTickerCount(rocksdb::MEMTABLE_MISS));
	printf("-----------------------------------------------------\n");
	DeallocBuffer(request_time_, (request_num_ + 1) * sizeof(double));
	// std::string stat_str2;
 // 	txn_db->GetProperty("rocksdb.stats", &stat_str2);
 // 	printf("\n%s\n", stat_str2.c_str());
}

void RocksDBClient::LoadThread(uint64_t num, int coreid){
	SetAffinity(coreid);
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
	rocksdb::get_perf_context()->Reset();
	rocksdb::get_iostats_context()->Reset();
	double *request_time = (double *)AllocBuffer((num + 1) * sizeof(double));

	for(uint64_t i=0; i<num; i++){
		std::string table;
		std::string key;
		std::vector<ycsbc::CoreWorkload::KVPair> values;
		workload_proxy_->LoadInsertArgs(table, key, values);
		assert(values.size() == 1);
		auto start = TIME_NOW;
		for (ycsbc::CoreWorkload::KVPair &field_pair : values) {
			std::string value = field_pair.second;
			ERR(txn_db->Put(write_options_, key, value));
		}
		request_time[i] = TIME_DURATION(start, TIME_NOW);
	}

	uint64_t pos = __sync_fetch_and_add(&request_time_num_, num);
	memcpy(request_time_ + pos, request_time, sizeof(double) * num);
	DeallocBuffer(request_time, (num + 1) * sizeof(double));
	mutex_.lock();
	wal_time_ += rocksdb::get_perf_context()->write_wal_time/1000.0;
	wait_time_ += rocksdb::get_perf_context()->write_thread_wait_nanos/1000.0;
	mutex_.unlock();
}

void RocksDBClient::WorkThread(uint64_t num, int coreid){
	SetAffinity(coreid);
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
	rocksdb::get_perf_context()->Reset();
	rocksdb::get_iostats_context()->Reset();

	double read_time = 0;
	double update_time = 0;
	uint64_t update_num = 0;
	uint64_t read_num = 0;
	double this_time = 0.0;
	
	double *request_time = (double *)AllocBuffer((num + 1) * sizeof(double));

	rocksdb::Transaction *txn = nullptr;
	for(uint64_t i=0; i<num; i++){
		ycsbc::Operation opt = workload_proxy_->GetNextOperation();
		if(opt == READ){
			GET_TIME_OUTPUT(Read(), this_time);
			read_time += this_time;
			read_num++;
			request_time[i] = this_time;
		}else if(opt == SCAN){
			GET_TIME_OUTPUT(Scan(), this_time);
			request_time[i] = this_time;
		}else if(opt == UPDATE){
			txn = txn_db->BeginTransaction(write_options_, txn_op_, txn);
			GET_TIME_OUTPUT(Update(txn), this_time);
			update_time += this_time;
			update_num++;
			request_time[i] = this_time;
		}else if(opt == INSERT){
			txn = txn_db->BeginTransaction(write_options_, txn_op_, txn);
			GET_TIME_OUTPUT(Insert(txn), this_time);
			update_time += this_time;
			update_num++;
			request_time[i] = this_time;
		}else if(opt == READMODIFYWRITE){
			txn = txn_db->BeginTransaction(write_options_, txn_op_, txn);
			GET_TIME_OUTPUT(ReadModifyWrite(txn), this_time);
			update_time += this_time;
			update_num++;
			request_time[i] = this_time;
		}else{
			throw utils::Exception("Operation request is not recognized!");
		}
	}

	uint64_t pos = __sync_fetch_and_add(&request_time_num_, num);
	memcpy(request_time_ + pos, request_time, sizeof(double) * num);
	DeallocBuffer(request_time, (num + 1) * sizeof(double));
	mutex_.lock();
	read_time_ += read_time;
	read_num_ += read_num;
	update_time_ += update_time;
	update_num_ += update_num;
	wal_time_ += rocksdb::get_perf_context()->write_wal_time/1000.0;
	wait_time_ += rocksdb::get_perf_context()->write_thread_wait_nanos/1000.0;
	block_read_time_ += rocksdb::get_perf_context()->block_read_time/1000.0;
	//printf("%s\n\n",rocksdb::get_perf_context()->ToString().c_str());
	//printf("%s\n\n",rocksdb::get_iostats_context()->ToString().c_str());
	mutex_.unlock();
}

void RocksDBClient::AsyncLoadThread(uint64_t num, int coreid){
	SetAffinity(coreid);
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
	rocksdb::get_perf_context()->Reset();
	rocksdb::get_iostats_context()->Reset();
	double *request_time = (double *)AllocBuffer((num + 1) * sizeof(double));
	//1. prepare pipeline
	TimePoint *senttime = new TimePoint[async_num_];
	rocksdb::Transaction* *txns = new rocksdb::Transaction* [async_num_];
	bool *occupied = new bool[async_num_];
	for(int i=0; i<async_num_; i++){
		txns[i] = txn_db->BeginTransaction(write_options_);
		occupied[i] = false;
	}
	uint64_t k=0;
	uint64_t j=0;
	//2. start
	while(true){
		//send a request
		for(int i=0; i<async_num_; i++){
			if(!occupied[i]){
				if(LIKELY(k < num)){
					occupied[i] = true;
					senttime[i] = TIME_NOW;
					std::string table;
					std::string key;
					std::vector<ycsbc::CoreWorkload::KVPair> values;
					txns[i] = txn_db->BeginTransaction(write_options_, txn_op_, txns[i]);
					workload_proxy_->LoadInsertArgs(table, key, values);
					assert(values.size() == 1);
					for (ycsbc::CoreWorkload::KVPair &field_pair : values) {
						std::string value = field_pair.second;
						ERR(txns[i]->Put(key, value));
					}
					ERR(txns[i]->AsyncCommit());
					k++;
				}
				break;
			}
		}
		//check completion
		bool finished = true;
		for(int i=0; i<async_num_; i++){
			if(occupied[i]){
				if(UNLIKELY(txns[i]->IsFinished())){
					request_time[j++] = TIME_DURATION(senttime[i], TIME_NOW);
					occupied[i] = false;
				}
				finished = false;
			}
		}
		//finish all requests
		if(UNLIKELY(finished && k==num))
			break;
	}

	assert(k==num && j==num);
	uint64_t pos = __sync_fetch_and_add(&request_time_num_, num);
	memcpy(request_time_ + pos, request_time, sizeof(double) * num);
	mutex_.lock();
	wal_time_ += rocksdb::get_perf_context()->write_wal_time/1000.0;
	wait_time_ += rocksdb::get_perf_context()->write_thread_wait_nanos/1000.0;
	mutex_.unlock();
	DeallocBuffer(request_time, (num + 1) * sizeof(double));
}

void RocksDBClient::AsyncWorkThread(uint64_t num, int coreid){
	SetAffinity(coreid);
	//1. prepare pipeline
	TimePoint *senttime = new TimePoint[async_num_];
	rocksdb::Transaction* *txns = new rocksdb::Transaction* [async_num_];
	bool *occupied = new bool[async_num_];
	for(int i=0; i<async_num_; i++){
		txns[i] = txn_db->BeginTransaction(write_options_);
		occupied[i] = false;
	}
	uint64_t k=0;
	ycsbc::Operation opt = NONE;
	double time = 0;
	double read_time = 0;
	double update_time = 0;
	uint64_t update_num = 0;
	uint64_t read_num = 0;
	double this_time = 0.0;
	//2. send request
	while(true){
		if(opt == NONE){
			opt = workload_proxy_->GetNextOperation();
			k++;
		}
		if(LIKELY(k <= num)){
			if(opt == READ){
				GET_TIME_OUTPUT(Read(), this_time);
				time += this_time;
				read_time += this_time;
				read_num++;
				opt = NONE;
			}else if(opt == SCAN){
				GET_TIME_OUTPUT(Scan(), this_time);
				time += this_time;
				opt = NONE;
			}else{
				//send a request
				for(int i=0; i<async_num_; i++){
					if(!occupied[i]){
						occupied[i] = true;
						senttime[i] = TIME_NOW;
						std::string table;
						std::string key;
						std::vector<ycsbc::CoreWorkload::KVPair> values;
						txns[i] = txn_db->BeginTransaction(write_options_, txn_op_, txns[i]);
						if(opt == UPDATE){
							workload_proxy_->GetUpdateArgs(table, key, values);
						}else if(opt == INSERT){
							workload_proxy_->GetInsertArgs(table, key, values);
						}else if(opt == READMODIFYWRITE){
							std::vector<std::string> fields;
							workload_proxy_->GetReadModifyWriteArgs(table, key, fields, values);
							std::string value;
							ERR(txns[i]->Get(read_options_, key, &value));
						}else{
							throw utils::Exception("Operation request is not recognized!");
						}
						assert(values.size() == 1);
						for (ycsbc::CoreWorkload::KVPair &field_pair : values) {
							std::string val = field_pair.second;
							txns[i]->Put(key, val);
						}
						txns[i]->AsyncCommit();
						opt = NONE;
						break;
					}
				}		
			}
		}
		//check completion
		bool finished = true;
		for(int i=0; i<async_num_; i++){
			if(occupied[i]){
				if(UNLIKELY(txns[i]->IsFinished())){
					double t = TIME_DURATION(senttime[i], TIME_NOW);
					time += t;
					update_time += t;
					update_num++;
					occupied[i] = false;
				}
				finished = false;
			}
		}
		//finish all requests
		if(UNLIKELY(finished && k>num))
			break;
	}
	mutex_.lock();
	total_time_ += time;
	read_time_ += read_time;
	read_num_ += read_num;
	update_time_ += update_time;
	update_num_ += update_num;
	wal_time_ += rocksdb::get_perf_context()->write_wal_time/1000.0;
	wait_time_ += rocksdb::get_perf_context()->write_thread_wait_nanos/1000.0;
	mutex_.unlock();
}

void RocksDBClient::Read(){
	std::string table;
	std::string key;
	std::vector<std::string> fields;
	std::string value;
	workload_proxy_->GetReadArgs(table, key, fields);
	txn_db->Get(read_options_, key, &value);
}

void RocksDBClient::ReadModifyWrite(rocksdb::Transaction *txn){
	std::string table;
	std::string key;
	std::vector<std::string> fields;
	std::vector<ycsbc::CoreWorkload::KVPair> values;
	workload_proxy_->GetReadModifyWriteArgs(table, key, fields, values);
	std::string value;
	txn->Get(read_options_, key, &value);
	assert(values.size() == 1);
	for (ycsbc::CoreWorkload::KVPair &field_pair : values) {
		std::string val = field_pair.second;
		ERR(txn->Put(key, val));
		//ERR(txn->Merge(key, val));
	}
	ERR(txn->Commit());
}

void RocksDBClient::Scan(){
	//TODO
}

void RocksDBClient::Insert(rocksdb::Transaction *txn){
	std::string table;
	std::string key;
	std::vector<ycsbc::CoreWorkload::KVPair> values;
	workload_proxy_->GetInsertArgs(table, key, values);
	assert(values.size() == 1);
	for (ycsbc::CoreWorkload::KVPair &field_pair : values) {
		std::string val = field_pair.second;
		ERR(txn->Put(key, val));
	}
	ERR(txn->Commit());
}

void RocksDBClient::Update(rocksdb::Transaction *txn){
	std::string table;
	std::string key;
	std::vector<ycsbc::CoreWorkload::KVPair> values;
	workload_proxy_->GetUpdateArgs(table, key, values);
	assert(values.size() == 1);
	for (ycsbc::CoreWorkload::KVPair &field_pair : values) {
		std::string val = field_pair.second;
		ERR(txn->Put(key, val));
	}
	ERR(txn->Commit());
}

void RocksDBClient::Reset(){
	total_time_ = 0;
	wait_time_ = wal_time_ = 0;
	update_time_ = read_time_ = 0;
	update_num_ = read_num_ = 0;
	block_read_time_ = 0;
	request_time_num_ = 0;
}

void RocksDBClient::PrintArgs(){
	if(txndb_options_.write_policy == 
		rocksdb::TxnDBWritePolicy::ASYNC_WRITE_PREPARED){
		printf("Async write prepared\n");
	}else if(txndb_options_.write_policy == 
		rocksdb::TxnDBWritePolicy::ASYNC_WRITE_COMMITTED){
		printf("Async write committed\n");
	}else if(txndb_options_.write_policy == 
		rocksdb::TxnDBWritePolicy::WRITE_PREPARED){
		printf("Write prepared\n");
	}else if(txndb_options_.write_policy == 
		rocksdb::TxnDBWritePolicy::WRITE_COMMITTED){
		printf("Write committed\n");
	}
	if(options_.ssdlogging_type == "spdk"){
		printf("log writer: %d, log threads: %d, ProLog: %d, EPiLog: %d, async_num: %d\n", 
				options_.ssdlogging_num, options_.logging_server_num, options_.before_server_num,
				options_.after_server_num, async_num_);
	}
	printf("WAL: %d, fsync: %d\n", !(write_options_.disableWAL), write_options_.sync);
	printf("Data: %s\n", data_dir_.c_str());
	printf("WAL: %s\n", options_.wal_dir.c_str());
	printf("Buffer size: %lld GB\n", options_.write_buffer_size/(1ull << 30));
}

}

