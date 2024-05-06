#include "rocksdb/cache.h"
#include "rocksdb_client2.h"
#include "iostream"
#include "cmath"
#include <sys/vfs.h> 
#include "rocksdb/table.h"
#include "properties.h"
#include "utils.h"
// #include "table/block_based/block_based_table_factory.h"

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
void CheckArgs(utils::Properties &props);
void PrintWorkload(const char* filename);
void Move(std::string src, std::string des, const int max_level);

int main(const int argc, const char *argv[]){
	utils::Properties props;
	ParseCommandLine(argc, argv, props);

	ycsbc::CoreWorkload wl;
	wl.Init(props);
	ycsbc::WorkloadProxy wp(&wl);

	const int loader_threads = stoi(props.GetProperty("threadcount"));
	const uint64_t load_num = stoull(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
	const int worker_threads = stoi(props.GetProperty("threadcount"));
	//const uint64_t requests_num = stoull(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
	const double buffer_size = stod(props.GetProperty("buffer_size"));
	const std::string log_dir = props.GetProperty("log_dir");
	const std::string data_dir = props.GetProperty("data_dir");
	const int is_load = stoi(props.GetProperty("is_load"));
	const std::string dbname = props.GetProperty("dbname");
	// const int max_level = stoi(props.GetProperty("max_level"));
    const int cache_size = stoi(props.GetProperty("cache_size"));
    
	const std::string cache_type = props.GetProperty("cache_type");
    //const int threads_num = stoi(props.GetProperty("threads_num"));
	const uint64_t warmup_num = stoull(props.GetProperty("warmup_num")) * 1000000;
	const uint64_t requests_num = stoull(props.GetProperty("work_request_num")) * 1000000;
    
	const bool cp_flag = (stoi(props.GetProperty("cp_flag")) != 0);
	const int req = stoi(props.GetProperty("req"));
	const int shard_bits = stoi(props.GetProperty("shard_bits"));
	printf("Cache sharding bits = %d\n", shard_bits);
	int async_num = 100;

	//===================common-setting==========
	rocksdb::Options options;
	rocksdb::WriteOptions write_options;
	rocksdb::ReadOptions read_options;
	options.allow_concurrent_memtable_write = true;
	options.recycle_log_file_num = false;
	options.allow_2pc = false;
	options.compression = rocksdb::kNoCompression;
	options.max_open_files = 500000;
	if(log_dir != "nologging"){
		write_options.sync = true;
		write_options.disableWAL = false;
        options.wal_dir = log_dir;
	}else{
		write_options.sync = false;
		write_options.disableWAL = true;
        options.wal_dir = data_dir;
	}
	if(is_load == 1){
		options.error_if_exists = true;
		options.create_if_missing = true;
	}else{
		options.error_if_exists = false;
		options.create_if_missing = true;
	}
	options.statistics = rocksdb::CreateDBStatistics();

	options.max_total_wal_size =  (uint64_t)(buffer_size * (1ull << 30));
	options.write_buffer_size = (uint64_t)(buffer_size * (1ull << 20)); //1ull << 32;

	auto env = rocksdb::Env::Default();
	options.env = env;
	env->SetBackgroundThreads(2, rocksdb::Env::HIGH);
	env->SetBackgroundThreads(6, rocksdb::Env::LOW);
	options.max_background_jobs = 8;
	options.max_subcompactions = 1;
	options.max_write_buffer_number = 4;

	if(cache_size == 0) {
		rocksdb::BlockBasedTableOptions table_options;
		table_options.no_block_cache = true;
		// table_options.block_cache = rocksdb::NewLRUCache(150 * (1ull<<30));
		options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
	} else if (cache_type.compare("LRU_FH") == 0) {
		auto cache = rocksdb::NewFHLRUCache(cache_size * (1ull<<30), shard_bits, false, 0);
		rocksdb::BlockBasedTableOptions table_options;
		table_options.block_cache = cache;
		auto table_factory = rocksdb::NewBlockBasedTableFactory(table_options);
		options.table_factory.reset(table_factory);
	} else {
		auto cache = rocksdb::NewLRUCache(cache_size * (1ull<<30), shard_bits, false, 0);
		rocksdb::BlockBasedTableOptions table_options;
		table_options.block_cache = cache;
		auto table_factory = rocksdb::NewBlockBasedTableFactory(table_options);
		options.table_factory.reset(table_factory);
	}

	// system(("rm " + data_dir + "/*").c_str());
	// system(("rm " + log_dir + "/*").c_str());
	// printf("loading database....\n");
	// if(is_load == 0)
	// 	system(("cp /home/spdk/raid5/rocksdb_100gb_bak/* " + data_dir + "/").c_str());
	// printf("loading finish\n");

  	//===================DB=======================================
  	// std::string pcie_addr = "";
  	printf("dbname: %s\n", dbname.c_str());
	if(dbname == "rocksdb"){
		// options.auto_config = false;
		// Ziyue add
		options.use_direct_io_for_flush_and_compaction = true;
		options.use_direct_reads = true;
	}
	// Run
	{
		// system("sync;echo 1 > /proc/sys/vm/drop_caches");
		// system("echo 2 > /proc/sys/vm/drop_caches");
		// system("echo 3 > /proc/sys/vm/drop_caches");
		fflush(stdout);
		printf("--------------memory usage----------------\n");
		fflush(stdout);
		auto res = system("free -h");
		fflush(stdout);
		printf("------------------------------------------\n");
		fflush(stdout);
		ycsbc::RocksDBClient2 rocksdb_client2(&wp, options, write_options, read_options, data_dir, loader_threads,
					  load_num, worker_threads, warmup_num, requests_num, async_num);
		if(is_load == 1){
			printf("Loading data from client\n");
			rocksdb_client2.Load();
		}else{
			printf("Running test for experiment\n");
			rocksdb_client2.Warmup();
			// options.lo_env->ResetStat();
			rocksdb_client2.Work();
			std::cout << "Finish experiment" << std::endl;
		}
		// rocksdb_client2.ProduceSPDKSST();
		//std::this_thread::sleep_for(std::chrono::seconds(300));
	}

	// delete
	if(dbname == "logsdb_ll0" || dbname == "logsdb_l0"){
		// delete options.lo_env;
	}
	fflush(stdout);
	return 0;
}

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
	if(argc != 15){
		printf("usage: <workload file> <threads num> <data_dir> <log_dir> \
					   <buffer size> <is_load> <dbname> \
                       <cache size> <warmup num> <worker request num> \
                       <cp or not> <request size> <seg num> <cache type>\n");
		exit(0);
	}
	//1. workload file
	std::ifstream input(argv[1]);
	try {
		props.Load(input);
	} catch (const std::string &message) {
		printf("%s\n", message.c_str());
		exit(0);
	}
	input.close();
	PrintWorkload(argv[1]);

	//2. threads number
	props.SetProperty("threadcount", argv[2]);

	//3. data directory
	props.SetProperty("data_dir", argv[3]);

	//4. log directory
	props.SetProperty("log_dir", argv[4]);

	//5. max buffer size
	props.SetProperty("buffer_size", argv[5]);

	//6. is_load
	props.SetProperty("is_load", argv[6]);

	//7. dbname
	props.SetProperty("dbname", argv[7]);

    //8. cache size
	props.SetProperty("cache_size", argv[8]);

    //9. warmup num
	props.SetProperty("warmup_num", argv[9]);

    //10. work request num
	props.SetProperty("work_request_num", argv[10]);

    //11. cp db or not
	props.SetProperty("cp_flag", argv[11]);

	//12. req size (Byte)
	props.SetProperty("req", argv[12]);

	//13. seg num
	props.SetProperty("shard_bits", argv[13]);

	//14. cache type
	props.SetProperty("cache_type", argv[14]);

	//check
	CheckArgs(props);
}

void CheckArgs(utils::Properties &props){
	;// TODO
}

void PrintWorkload(const char* filename){
	FILE *file = fopen(filename, "r");
	char line[201];
	auto x = fgets(line,200,file);
	printf("==================Workload=================\n");
	printf("%s\n", filename);
	while(!feof(file)){
		std::string s = std::string(line);
		if(s.find("#") != 0 && s != "\n" && s!=""){
			printf("%s", s.c_str());
		}
		auto y = fgets(line,200,file);
	}
	fclose(file);
	printf("==========================================\n");
	fflush(stdout);
}