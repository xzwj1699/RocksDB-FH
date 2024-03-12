#pragma once

#include "ssdlogging/spdk_device.h"
#include "ssdlogging/util.h"
#include "utilities/lock_free_queue/disruptor_queue.h"
#include "port/likely.h"
#include "rocksdb/status.h"
#include "rocksdb/env.h"
#include "port/port.h"
#include "util/murmurhash.h"
#include "util/coding.h"
#include "rocksdb/options.h"

#include "algorithm"
#include "chrono"
#include "port/sys_time.h"
#include "util/cast_util.h"
#include "util/random.h"
#include "util/rate_limiter.h"
#include <condition_variable>

#define SPDK_PAGE_SIZE (4096)
#define FILE_BUFFER_ENTRY_SIZE (1ull<<20)
#define SPDK_MEM_POOL_SIZE (60ull<<30)
#define SPDK_MEM_POOL_ENTRY_NUM (SPDK_MEM_POOL_SIZE/FILE_BUFFER_ENTRY_SIZE)
#define LO_START_LPN ((20ull<<30)/(SPDK_PAGE_SIZE))
// #define L0_MAX_LPN ((1200ull<<30)/(SPDK_PAGE_SIZE))
#define META_SIZE (1<<20)
#define LO_FILE_START (LO_START_LPN + (META_SIZE)/(SPDK_PAGE_SIZE))
#define HUGE_PAGE_SIZE (1<<28)
#define ExitError() do{Exit();exit(-1);}while(0);
// #define WAIT_CHECKING

namespace rocksdb{
struct SPDKBuffer;
struct SPDKFileMeta;
struct BufferPool;
struct HugePage;
class SpdkFile;

static ssdlogging::SPDKInfo *sp_info_ = nullptr;
static int l0_queue_num_ = 0;
static int total_queue_num_ = 0;
static uint64_t L0_MAX_LPN;
static int sync_io_queue_id_ = 0;

static BufferPool *spdk_mem_pool_ = nullptr;
static HugePage *huge_pages_ = nullptr;

typedef std::map<std::string, SPDKFileMeta*> FileMeta;
typedef std::map<std::string, SpdkFile*> FileSystem;
FileMeta file_meta_;
FileSystem file_system_;
port::Mutex fs_mutex_;
port::Mutex meta_mutex_;
char *meta_buffer_;
static uint64_t spdk_tsc_rate_;
static port::Mutex **spdk_queue_mutexes_ = nullptr;
static uint64_t spdk_queue_id_ = 0;
static std::atomic<bool> stop_;

#ifdef WAIT_CHECKING
std::thread check_thread_;
#endif

static void Exit();
static uint64_t spdkfile_access_time_ = 0;
static void EvictFileData();

struct LatencyRecord{
  uint64_t total_;
  uint64_t num_;
  LatencyRecord():total_(0), num_(0){};
  void Add(uint64_t value){
    __sync_fetch_and_add(&total_, value);
    __sync_fetch_and_add(&num_, 1);
  }
  double Avg(){ return total_*1.0/num_; }
  uint64_t Num(){ return num_;}
};

#ifdef LOGSDB_STAT 
static LatencyRecord write_latency_;
static LatencyRecord read_latency_;
static LatencyRecord read_miss_latency_;
static uint64_t read_hit_;
static uint64_t read_miss_;
#endif

struct HugePage{
  public:
    HugePage(uint64_t size):
      size_(size),
      hugepages_(nullptr),
      index_(0),
      offset_(0){
        page_num_ = size_/HUGE_PAGE_SIZE;
        if(size % HUGE_PAGE_SIZE != 0 )
          page_num_++;
        hugepages_ = new char*[page_num_];
        printf("SPDK memory allocation starts (%.2lf GB)\n", size_/1024.0/1024.0/1024.0);
        for(uint64_t i=0; i<page_num_; i++){
          if(i % (uint64_t)(page_num_*0.2) == 0){
            printf("...%.1f%%", i*1.0/page_num_ * 100);
            fflush(stdout);
          }
          hugepages_[i] = (char*)spdk_zmalloc(HUGE_PAGE_SIZE, SPDK_PAGE_SIZE, NULL, 
                                              SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
          if(UNLIKELY(hugepages_[i] == nullptr)){
            printf("\n");
            fprintf(stderr, "already allocated %.2lf GB\n", HUGE_PAGE_SIZE * i/1024.0/1024.0/1024.0);
            fprintf(stderr, "%s:%d: SPDK allocate memory failed\n", __FILE__, __LINE__);
            ExitError();
          }
        }
        printf("...100%%\nSPDK memory allocation finished.\n");
      };
    char *Get(uint64_t size){
      assert(size <= HUGE_PAGE_SIZE);
      if(HUGE_PAGE_SIZE - offset_ < size){
        index_++;
        offset_ = 0;
      }
      if(index_ >= page_num_)
        return nullptr;
      char *data = hugepages_[index_] + offset_;
      offset_ += size;
      return data;
    }
    ~HugePage(){
      for(uint64_t i=0; i<page_num_; i++){
        if(hugepages_[i] != nullptr)
          spdk_free(hugepages_[i]);
      }
    }
  private:
    uint64_t size_;
    uint64_t page_num_;
    char **hugepages_;
    uint64_t index_;
    uint64_t offset_;
};

//mem pool entry
struct SPDKBuffer{
  private:
    char *buffer_;
    const uint64_t max_size_ = FILE_BUFFER_ENTRY_SIZE;
    uint64_t offset_;
  public:
    SPDKBuffer(){
        buffer_ = huge_pages_->Get(FILE_BUFFER_ENTRY_SIZE);
        // buffer_ = (char*)spdk_zmalloc(FILE_BUFFER_ENTRY_SIZE, 
        //                               SPDK_PAGE_SIZE, NULL, 
        //                               SPDK_ENV_SOCKET_ID_ANY,
        //                               SPDK_MALLOC_DMA);
        if(UNLIKELY(buffer_ == nullptr)){
            fprintf(stderr, "%s:%d: SPDK allocate memory failed\n", __FILE__, __LINE__);
            ExitError();
        }
        offset_ = 0;
    }
    size_t Append(const char* data, size_t size){
        size_t s = 0;
        if(offset_ == max_size_)
            return s;
        if(offset_ + size > max_size_)
            s = max_size_ - offset_;
        else
            s = size;
        memcpy(buffer_ + offset_, data, s);
        offset_ += s;
        return s;
    }
    void ReadData(char* ptr, size_t offset, size_t size){
        assert(offset + size <= max_size_);
        memcpy(ptr, buffer_ + offset, size);
    }
    bool Full(){ return offset_ == max_size_; }
    bool Empty(){ return offset_ == 0; }
    char *Data(){ return buffer_; }
    uint64_t Size(){ return offset_; }
    uint64_t MaxSize(){ return max_size_;  }
    void Reset(){ offset_ = 0; }
    
};

struct BufferPool{
 private:
  const int64_t size_;
  std::atomic<int64_t> last_read_;
  std::atomic<int64_t> last_wrote_;
  std::atomic<int64_t> next_write_;
  std::atomic<int64_t> next_read_;
  port::Mutex mutex_;
  SPDKBuffer **buffer_;
 public:
  BufferPool(int64_t size):
    size_(size),
    last_read_(-1),
    last_wrote_(-1),
    next_write_(0),
    next_read_(-1){
      buffer_ = new SPDKBuffer*[size_];
      for(int64_t i=0; i<size_; i++)
        buffer_[i] = nullptr;
  }
  ~BufferPool(){
    delete buffer_;
  }
  void Put(SPDKBuffer* value){
    const int64_t seq = next_write_.fetch_add(1);
		while (seq - last_read_.load() > size_){
    
		}
		buffer_[seq % size_] = value;
		while (seq - 1 != last_wrote_.load()){

		}
		last_wrote_.store(seq);
  }
  SPDKBuffer* Get(){
    int64_t seq = -1;
    {
      MutexLock lock(&mutex_);
			seq = next_read_.load() + 1;
			if(seq > last_wrote_.load()){
				return nullptr;
			}
			next_read_.fetch_add(1);
		}
		assert(seq >= 0);
		SPDKBuffer *value =  buffer_[seq % size_];
		while (seq - 1 != last_read_.load()){

    }
		last_read_.store(seq);
		return value;
  }
};

//file buffer entry
struct FileBuffer{
  SPDKBuffer *spdk_buffer_;
  int queue_id_;
  uint64_t start_lpn_;
  bool synced_;
  bool readable_;
  uint64_t read_size_ = 0;
  std::mutex readable_mu_;
  std::condition_variable readable_cv_;
  std::atomic<uint64_t> *file_pending_sync_num_;
  std::mutex *file_sync_mu_;
  std::condition_variable *file_sync_cv_;
  uint64_t start_time_;
  explicit FileBuffer(uint64_t start_lpn, std::atomic<uint64_t> *pending_num,
                      std::mutex *file_sync_mu, std::condition_variable *file_sync_cv):
        spdk_buffer_(nullptr),
        queue_id_(-1),
        start_lpn_(start_lpn),
        synced_(false),
        readable_(false),
        file_pending_sync_num_(pending_num),
        file_sync_mu_(file_sync_mu),
        file_sync_cv_(file_sync_cv){}

  bool AllocateSPDKBuffer(){
    assert(spdk_buffer_ == nullptr);
    spdk_buffer_ = spdk_mem_pool_->Get();
    if(spdk_buffer_ == nullptr){
      EvictFileData();
      spdk_buffer_ = spdk_mem_pool_->Get();
      if(spdk_buffer_ == nullptr){
          printf("Can not get buffer after delete data\n");
          printf("SPDK metadata size: %ld\n", file_meta_.size());
          printf("File system size: %ld\n", file_system_.size());
          ExitError();
      }
    }
    spdk_buffer_->Reset();
    queue_id_ = -1;
    readable_ = false;
    synced_ = false;
    return true;
  }

  void FreeSPDKBuffer(){
    if(spdk_buffer_ == nullptr)
      return ;
    spdk_mem_pool_->Put(spdk_buffer_);
    spdk_buffer_ = nullptr;
    readable_ = false;
    file_pending_sync_num_ = nullptr;
    file_sync_cv_ = nullptr;
    file_sync_mu_ = nullptr;
  }

  bool Full(){
        assert(spdk_buffer_ != nullptr);
        return spdk_buffer_->Full(); 
    }
  bool Empty(){
        assert(spdk_buffer_ != nullptr);
        return spdk_buffer_->Empty(); 
    }
  size_t Append(const char* data, size_t size){
        assert(spdk_buffer_ != nullptr);
        return spdk_buffer_->Append(data, size);
    }
  void ReadData(char* ptr, size_t offset, size_t size){
        assert(spdk_buffer_ != nullptr);
        return spdk_buffer_->ReadData(ptr, offset, size);
    }
};

struct SPDKFileMeta{
  std::string fname_;
  uint64_t start_lpn_;
  uint64_t end_lpn_;
  bool lock_file_;
  uint64_t size_;
  uint64_t modified_time_;
  uint64_t last_access_time_;
  SPDKFileMeta(){};
  SPDKFileMeta(std::string fname, uint64_t start,
               uint64_t end, bool lock_file):
        fname_(fname),
        start_lpn_(start),
        end_lpn_(end),
        lock_file_(lock_file),
        size_(0),
        modified_time_(0),
        last_access_time_(0){}
  SPDKFileMeta(std::string fname, uint64_t start,
               uint64_t end, bool lock_file, uint64_t size):
        fname_(fname),
        start_lpn_(start),
        end_lpn_(end),
        lock_file_(lock_file),
        size_(size),
        modified_time_(0),
        last_access_time_(0){}

  uint64_t Size(){return size_; };
  
  uint64_t ModifiedTime(){return modified_time_;}

  uint32_t Serialization(char* des){
    uint32_t offset = 0;
    uint64_t size = fname_.size();
    EncodeFixed64(des + offset, size);
    offset += 8;
    memcpy(des + offset, fname_.c_str(), size);
    offset += size;
    EncodeFixed64(des + offset, start_lpn_);
    offset += 8;
    EncodeFixed64(des + offset, end_lpn_);
    offset += 8;
    char lock = lock_file_ ? '1' : '0';
    memcpy(des + offset, &lock, 1);
    offset += 1;
    EncodeFixed64(des + offset, size_);
    offset += 8;
    EncodeFixed64(des + offset, modified_time_);
    offset += 8;
    return offset;   
  }

  uint32_t Deserialization(char *src){
    uint32_t offset = 0;
    uint64_t size = 0;
    size = DecodeFixed64(src + offset);
    offset += 8;
    fname_ = std::string(src + offset, size);
    offset += fname_.size();
    start_lpn_ = DecodeFixed64(src + offset);
    offset += 8;
    end_lpn_ = DecodeFixed64(src + offset);
    offset += 8;
    char lock;
    memcpy(&lock, src + offset, 1);
    lock_file_ = (lock == '1');
    offset += 1;
    size_ = DecodeFixed64(src + offset);
    offset += 8;
    modified_time_ = DecodeFixed64(src + offset);
    offset += 8;
    return offset;
  }

  std::string ToString(){
    char out[200];
    sprintf(out, "FileName: %s, StartLPN: %ld, EndLPN: %ld, IsLock: %d, Size: %ld, ModifiedTime: %ld\n",
            fname_.c_str(), start_lpn_, end_lpn_, lock_file_,
            size_, modified_time_);
    return std::string(out);
  }
};

static bool SPDKWrite(ssdlogging::SPDKInfo *spdk, FileBuffer *buf);

#ifdef WAIT_CHECKING
static void BusyCheckCompletion(){
  // ssdlogging::SetAffinity(3);
  while(true){
    if(stop_.load())
      break;
    for(int i=0; i<l0_queue_num_; i++){
      int queueid = total_queue_num_ - i - 1;
      MutexLock lock(spdk_queue_mutexes_[queueid]);
      spdk_nvme_qpair_process_completions(sp_info_->namespaces->qpair[queueid], 0);
    }
  }
}
#endif

static void Init(std::string pcie_addr, int logging_queue_num, int l0_queue_num){
  sp_info_ = ssdlogging::InitSPDK(pcie_addr, logging_queue_num);
  L0_MAX_LPN = ((uint64_t)(sp_info_->namespaces->capacity * 0.8))/SPDK_PAGE_SIZE;
  spdk_tsc_rate_ = spdk_get_ticks_hz();
  assert(sp_info_ != nullptr);
  total_queue_num_ = sp_info_->num_io_queues;//one for metadata
  sync_io_queue_id_ = total_queue_num_ - 1;
  l0_queue_num_ = l0_queue_num;
  if(total_queue_num_ < l0_queue_num + logging_queue_num + 1){
    fprintf(stderr, "total queue num (%d) < l0 queue num (%d) + logging queue num (%d) + 1\n", 
            total_queue_num_, l0_queue_num_, logging_queue_num);
    fprintf(stderr, "one queue is dedicated for logging metadata\n");
    ExitError();
  }
  printf("total queue: %d, L0 queue: %d\n", total_queue_num_, l0_queue_num_);
  //1.Allocate mem pool
  huge_pages_ = new HugePage(SPDK_MEM_POOL_SIZE);
  spdk_mem_pool_ = new BufferPool((int64_t)SPDK_MEM_POOL_ENTRY_NUM);
  for(uint64_t i = 0; i < SPDK_MEM_POOL_ENTRY_NUM; i++){
    SPDKBuffer *buffer = new SPDKBuffer();
    spdk_mem_pool_->Put(buffer);
  }
  //2.Allocate meta buffer
  meta_buffer_  = (char*)spdk_zmalloc(META_SIZE, 
                                      SPDK_PAGE_SIZE, NULL, 
                                      SPDK_ENV_SOCKET_ID_ANY,
                                      SPDK_MALLOC_DMA);
  if(UNLIKELY(meta_buffer_ == nullptr)){
    fprintf(stderr, "%s:%d: SPDK allocate memory failed\n", __FILE__, __LINE__);
    ExitError();
  }
  //3.Initialize queue mutex and  queue stat
  spdk_queue_mutexes_ = new port::Mutex* [total_queue_num_];
  for(int i=0; i<total_queue_num_; i++){
    spdk_queue_mutexes_[i] = new port::Mutex();
  }
  #ifdef WAIT_CHECKING
    stop_.store(false);
    check_thread_ = std::thread(BusyCheckCompletion);
  #endif
}

static uint64_t GetSPDKQueueID(){
  return total_queue_num_ - (__sync_fetch_and_add(&spdk_queue_id_, 1) % l0_queue_num_) - 2;
}

static void WriteComplete(void *arg, const struct spdk_nvme_cpl *completion){
  FileBuffer *file_buf = (FileBuffer *)arg;
  assert(file_buf->synced_ == false);
  if (spdk_nvme_cpl_is_error(completion)) {
        fprintf(stderr, "I/O error status: %s\n", 
                    spdk_nvme_cpl_get_status_string(&completion->status));
        fprintf(stderr, "%s:%d: Write I/O failed, aborting run\n", __FILE__, __LINE__);
        file_buf->synced_ = false;
        ExitError();
  }

  #ifdef LOGSDB_STAT
  uint64_t time = (uint64_t)(SPDK_TIME_DURATION(file_buf->start_time_, SPDK_TIME, spdk_tsc_rate_)*1000);
  write_latency_.Add(time);
  #endif

  file_buf->synced_ = true;
  #ifdef WAIT_CHECKING
    uint64_t left = file_buf->file_pending_sync_num_->fetch_sub(1) - 1;
    if(left == 0){
      std::unique_lock<std::mutex> lock(*(file_buf->file_sync_mu_));
      file_buf->file_sync_cv_->notify_one();
    }
  #endif
}

static void ReadComplete(void *arg, const struct spdk_nvme_cpl *completion){
  FileBuffer *file_buf = (FileBuffer *)arg;
  assert(file_buf->readable_ == false);
  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n", 
              spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "%s:%d: Write I/O failed, aborting run\n", __FILE__, __LINE__);
    file_buf->synced_ = false;
    ExitError();
  }
  #ifdef WAIT_CHECKING
    {
      std::unique_lock<std::mutex> lock(file_buf->readable_mu_);
      file_buf->readable_ = true;
    }
    file_buf->readable_cv_.notify_one();
  #else
    file_buf->readable_ = true;
  #endif
}

static void CheckComplete(ssdlogging::SPDKInfo *spdk, int queue_id){
  assert(queue_id != -1);
  MutexLock lock(spdk_queue_mutexes_[queue_id]);
  spdk_nvme_qpair_process_completions(spdk->namespaces->qpair[queue_id], 0);
}

static bool SPDKWrite(ssdlogging::SPDKInfo *spdk, FileBuffer *buf){
  // ssdlogging::ns_entry *namespace = spdk->namespaces;
	assert(buf != NULL);
  assert(buf->synced_ == false);
	assert(buf->spdk_buffer_ != NULL);
  //assert(buf->spdk_buffer_->offset_ <= buf->spdk_buffer_->max_size_);
  uint64_t sec_per_page = SPDK_PAGE_SIZE / spdk->namespaces->sector_size;
  uint64_t lba = buf->start_lpn_ * sec_per_page;
  uint64_t size = buf->spdk_buffer_->Size();
  if(size % SPDK_PAGE_SIZE != 0)
    size = (size / SPDK_PAGE_SIZE + 1) * SPDK_PAGE_SIZE;
  uint64_t sec_num = size / SPDK_PAGE_SIZE * sec_per_page;
  int queue_id = GetSPDKQueueID();
  buf->queue_id_ = queue_id;
  #ifdef LOGSDB_STAT 
    buf->start_time_ = SPDK_TIME;
  #endif
  //printf("write lba: %ld, size: %ld sectors, queue: %d\n", lba, sec_num, queue_id);
  #ifdef WAIT_CHECKING
    buf->file_pending_sync_num_->fetch_add(1);
  #endif
  int rc = 0;
  {
    MutexLock lock(spdk_queue_mutexes_[queue_id]);
    rc = spdk_nvme_ns_cmd_write(spdk->namespaces->ns,
                    spdk->namespaces->qpair[queue_id],
                    buf->spdk_buffer_->Data(),
					          lba, /* LBA start */
					          sec_num, /* number of LBAs */
					          WriteComplete,
                    buf, 0);
  }
  if (rc != 0){
    fprintf(stderr, "%s:%d: SPDK IO failed: %s\n", __FILE__, __LINE__, strerror(rc * -1));
    ExitError();
	}
  return true;
}

static bool SPDKRead(ssdlogging::SPDKInfo *spdk, FileBuffer *buf){
  // ssdlogging::ns_entry* namespace = spdk->namespaces;
	assert(buf != NULL);
  assert(buf->readable_ == false);
	assert(buf->spdk_buffer_ != NULL);
  uint64_t sec_per_page = SPDK_PAGE_SIZE / spdk->namespaces->sector_size;
  uint64_t lba = buf->start_lpn_ * sec_per_page;
  uint64_t sec_num = buf->spdk_buffer_->MaxSize() / SPDK_PAGE_SIZE * sec_per_page;
  int queue_id = GetSPDKQueueID();
  buf->queue_id_ = queue_id;
  //printf("read lba: %ld, size: %ld sectors, queue: %d\n", lba, sec_num, queue_id);
  int rc = 0;
  {
    MutexLock lock(spdk_queue_mutexes_[queue_id]);
    rc = spdk_nvme_ns_cmd_read(spdk->namespaces->ns,
          spdk->namespaces->qpair[queue_id],
          buf->spdk_buffer_->Data(),
					lba, /* LBA start */
					sec_num, /* number of LBAs */
					ReadComplete,
          buf, 0);
  }
  if (rc != 0){
    fprintf(stderr, "%s:%d: SPDK IO failed: %s\n", __FILE__, __LINE__, strerror(rc * -1));
    ExitError();
  }
  return true;
}

static void SPDKWriteSync(ssdlogging::SPDKInfo *spdk, char *buf, uint64_t lpn, uint64_t size){
  assert(size % SPDK_PAGE_SIZE == 0);
  uint64_t sec_per_page = SPDK_PAGE_SIZE / spdk->namespaces->sector_size;
  uint64_t lba = lpn * sec_per_page;
  uint64_t sec_num = size / SPDK_PAGE_SIZE * sec_per_page;
  // int queue_id = GetSPDKQueueID();
  int queue_id = sync_io_queue_id_;
  bool flag = false;
  // printf("write lba: %ld, size: %ld sectors\n", lba, sec_num);
  int rc = spdk_nvme_ns_cmd_write(spdk->namespaces->ns,
                    spdk->namespaces->qpair[queue_id],
                    buf,
					          lba, /* LBA start */
					          sec_num, /* number of LBAs */
					          [](void *arg, const struct spdk_nvme_cpl *completion) -> void {
                      bool *finished = (bool *)arg;
                      if (UNLIKELY(spdk_nvme_cpl_is_error(completion))) {
                        fprintf(stderr, "I/O error status: %s\n", 
                                    spdk_nvme_cpl_get_status_string(&completion->status));
                        fprintf(stderr, "%s:%d: Write I/O failed, aborting run\n", __FILE__, __LINE__);
                        *finished = true;
                        ExitError();
                      }
                      *finished = true;
                    },
                    &flag, 0);
  if (rc != 0){
    fprintf(stderr, "%s:%d: SPDK IO failed: %s\n", __FILE__, __LINE__, strerror(rc * -1));
    ExitError();
	}
  while(!flag){
    CheckComplete(sp_info_, queue_id);
  }
}

static void SPDKReadSync(ssdlogging::SPDKInfo *spdk, char *buf, uint64_t lpn, uint64_t size){
  uint64_t sec_per_page = SPDK_PAGE_SIZE / spdk->namespaces->sector_size;
  uint64_t lba = lpn * sec_per_page;
  uint64_t sec_num = size / SPDK_PAGE_SIZE * sec_per_page;
  // int queue_id = GetSPDKQueueID();
  int queue_id = sync_io_queue_id_;
  bool flag = false;
  // printf("write lba: %ld, size: %ld sectors\n", lba, sec_num);
  int rc = spdk_nvme_ns_cmd_read(spdk->namespaces->ns,
                    spdk->namespaces->qpair[queue_id],
                    buf,
					          lba, /* LBA start */
					          sec_num, /* number of LBAs */
					          [](void *arg, const struct spdk_nvme_cpl *completion) -> void {
                      bool *finished = (bool *)arg;
                      if (UNLIKELY(spdk_nvme_cpl_is_error(completion))) {
                        fprintf(stderr, "I/O error status: %s\n", 
                                    spdk_nvme_cpl_get_status_string(&completion->status));
                        fprintf(stderr, "%s:%d: Write I/O failed, aborting run\n", __FILE__, __LINE__);
                        *finished = true;
                        ExitError();
                      }
                      *finished = true;
                    },
                    &flag, 0);
  if (rc != 0){
    fprintf(stderr, "%s:%d: SPDK IO failed: %s\n", __FILE__, __LINE__, strerror(rc * -1));
    ExitError();
	}
  while(!flag){
    CheckComplete(sp_info_, queue_id);
  }
}

static void WriteMeta(FileMeta *file_meta){
  MutexLock lock(&meta_mutex_);
  assert(meta_buffer_ != nullptr);
  uint64_t size = 0;
  EncodeFixed32(meta_buffer_, (uint32_t)file_meta->size());
  size += 4;
  //printf("---------------------\n");
  //printf("write meta num: %ld\n", file_meta->size());
  for(auto &meta : *file_meta){
    size += meta.second->Serialization(meta_buffer_ + size);
    //printf("%s\n", meta.second->ToString().c_str());
  }
  //printf("---------------------\n");
  if(size % SPDK_PAGE_SIZE != 0)
    size = (size / SPDK_PAGE_SIZE + 1) * SPDK_PAGE_SIZE;
  assert(size <= META_SIZE);
  if(UNLIKELY(size > META_SIZE)){
    fprintf(stderr, "%s:%d: Metadata size is too big\n", __FILE__, __LINE__);
    ExitError();
  }
  SPDKWriteSync(sp_info_, meta_buffer_, LO_START_LPN, size);
}

static std::string SplitFname(std::string path){
  std::size_t found = path.find_last_of("/");
  return path.substr(found+1);
  
}

static void ReadMeta(FileMeta *file_meta, std::string dbpath){
  SPDKReadSync(sp_info_, meta_buffer_, LO_START_LPN, META_SIZE);
  uint64_t offset = 0;
  uint32_t meta_num = DecodeFixed32(meta_buffer_ + offset);
  offset += 4;
  //printf("---------------------\n");
  // printf("read meta num: %d\n", meta_num);
  for(uint32_t i=0; i<meta_num; i++){
    assert(offset < META_SIZE);
    SPDKFileMeta *meta = new SPDKFileMeta();
    offset += meta->Deserialization(meta_buffer_ + offset);
    // printf("fname: %s\n", SplitFname(meta->fname_).c_str());
    meta->fname_ = dbpath + "/" + SplitFname(meta->fname_);
    (*file_meta)[meta->fname_] = meta;
    //printf("%s\n", meta->ToString().c_str());
  }
  //printf("---------------------\n");
}

class SpdkFile {
 public:
	explicit SpdkFile(Env* env, const std::string& fname, SPDKFileMeta *metadata, uint64_t allocate_size)
      : env_(env),
        fname_(fname),
        rnd_(static_cast<uint32_t>(
            MurmurHash(fname.data(), static_cast<int>(fname.size()), 0))),
        current_buf_index_(0),
        last_sync_index_(-1),
        refs_(0),
        metadata_(metadata),
        pending_sync_num_(0){
    //printf("create spdk file fname: %s, ref: %ld\n", fname_.c_str(), refs_);
		uint64_t current_page = metadata->start_lpn_;
		uint64_t pages_per_buffer = FILE_BUFFER_ENTRY_SIZE / SPDK_PAGE_SIZE;
    file_buffer_num_ = allocate_size / FILE_BUFFER_ENTRY_SIZE;
    if(allocate_size % FILE_BUFFER_ENTRY_SIZE != 0)
      file_buffer_num_++;
    //printf("open spdkfile: %s\n", metadata_->ToString().c_str());
    //printf("file_buffer_num_: %ld\n", file_buffer_num_);
		file_buffer_ = (FileBuffer **)malloc(sizeof(FileBuffer) * file_buffer_num_);
		for(uint64_t i=0; i<file_buffer_num_; i++){
			file_buffer_[i] = new FileBuffer(current_page, &pending_sync_num_, &pending_sync_mu_, &pending_sync_cv_);
			current_page += pages_per_buffer;
		}
    Ref();
	}

	SpdkFile(const SpdkFile&) = delete; // No copying allowed.
	void operator=(const SpdkFile&) = delete;

	~SpdkFile(){
    assert(refs_ == 0);
		for(uint64_t i=0; i<file_buffer_num_; i++){
      if(file_buffer_[i]->spdk_buffer_ != nullptr){
        file_buffer_[i]->FreeSPDKBuffer();
      }
			delete file_buffer_[i];
		}
		free(file_buffer_);
    assert(file_meta_.find(fname_) != file_meta_.end());
    //WriteMeta(&file_meta_); 
    // printf("delete spdk file: %s, size: %ld\n", fname_.c_str(), size_.load());
    //printf("delete spdk file fname: %s, ref: %ld\n", fname_.c_str(), refs_);
	}

	bool is_lock_file() const { return metadata_->lock_file_; }

	bool Lock() {
    assert(metadata_->lock_file_);
    MutexLock lock(&mutex_);
    if (locked_) {
    	return false;
    } else {
    	locked_ = true;
    	return true;
    }
  }

	void Unlock() {
    	assert(metadata_->lock_file_);
    	MutexLock lock(&mutex_);
    	locked_ = false;
	}

  uint64_t Size() const { return metadata_->size_;}

  void Truncate(size_t size) {
		MutexLock lock(&mutex_);
		//TODO
		fprintf(stderr, "%s:%d: SPDK File Truncate() not implemented\n", __FILE__, __LINE__);
	}

	void CorruptBuffer() {
		//TODO
		fprintf(stderr, "%s:%d: SPDK File CorruptBuffer() not implemented\n", __FILE__, __LINE__);
  }

	Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) {
   #ifdef LOGSDB_STAT
    auto start = SPDK_TIME;
   #endif
    //MutexLock lock(&mutex_);
    metadata_->last_access_time_ = __sync_add_and_fetch(&spdkfile_access_time_, 1);
    assert(scratch != nullptr);
		size_t size = 0;
    char* ptr = scratch;
		while(n > 0){
			int64_t buffer_index =  offset / FILE_BUFFER_ENTRY_SIZE;
			uint64_t buffer_offset = offset % FILE_BUFFER_ENTRY_SIZE;
      assert(buffer_index < file_buffer_num_);
			if(file_buffer_[buffer_index]->spdk_buffer_ == nullptr){
        bool r = file_buffer_[buffer_index]->AllocateSPDKBuffer();
        assert(r);
        // printf("read allocate buffer %s: %ld\n", fname_.c_str(), buffer_index);
			}
      //read from device
			if(!file_buffer_[buffer_index]->readable_){
        // printf("read from %s: %ld\n", fname_.c_str(), buffer_index);
        #ifdef LOGSDB_STAT
          auto s = SPDK_TIME;
        #endif
        SPDKRead(sp_info_, file_buffer_[buffer_index]);
        #ifdef WAIT_CHECKING
          {
            std::unique_lock<std::mutex> lk(file_buffer_[buffer_index]->readable_mu_);
            file_buffer_[buffer_index]->readable_cv_.wait(lk, [&]{return file_buffer_[buffer_index]->readable_;});
          }
        #else
          while(!file_buffer_[buffer_index]->readable_){
            CheckComplete(sp_info_, file_buffer_[buffer_index]->queue_id_);
          }
        #endif
        #ifdef LOGSDB_STAT
          __sync_fetch_and_add(&read_miss_, 1);
          uint64_t t = (uint64_t)(SPDK_TIME_DURATION(s, SPDK_TIME, spdk_tsc_rate_)*1000);
          read_miss_latency_.Add(t);
        #endif
			}
      #ifdef LOGSDB_STAT
      else{
        __sync_fetch_and_add(&read_hit_, 1);
      }
      #endif
      //copy to ptr
			size_t s = 0;
			if(buffer_offset + n <= FILE_BUFFER_ENTRY_SIZE){
				s = n;
			}else{
			  s = FILE_BUFFER_ENTRY_SIZE - buffer_offset;
			}
      file_buffer_[buffer_index]->ReadData(ptr + size, buffer_offset, s);
      size += s;
      offset += s;
      n -= s;
		}
    assert(n == 0);
    *result = Slice(scratch, size);
    #ifdef LOGSDB_STAT
      uint64_t time = (uint64_t)(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_)*1000);
      read_latency_.Add(time);
    #endif
		return Status::OK();
	}

	Status Write(uint64_t offset, const Slice& data) {
		//TODO
		fprintf(stderr, "%s:%d: SPDK File Write() not implemented\n", __FILE__, __LINE__);
		return Status::OK();
	}

	Status Append(const Slice& data) {
    metadata_->last_access_time_ = __sync_add_and_fetch(&spdkfile_access_time_, 1);
		// MutexLock lock(&mutex_);
		size_t left = data.size();
		size_t written = 0;
		while(left > 0){
      if(UNLIKELY((uint64_t)current_buf_index_ >= file_buffer_num_)){
        fprintf(stderr, "File size is too big. Allocate size: %ld, Current size: %ld, write size: %ld(left: %ld)\n",
                file_buffer_num_ * FILE_BUFFER_ENTRY_SIZE, metadata_->size_, data.size(), left);
        ExitError();
      }
      // printf("fname: %s, write size: %ld, buf index: %d\n",fname_.c_str(), data.size(), current_buf_index_);
			const char *dat = data.data() + written;
      if(file_buffer_[current_buf_index_]->spdk_buffer_ == nullptr){
        bool r = file_buffer_[current_buf_index_]->AllocateSPDKBuffer();
        assert(r);
			}
      file_buffer_[current_buf_index_]->readable_ = true;
			size_t size = file_buffer_[current_buf_index_]->Append(dat, left);
			left -= size;
			written += size;
			assert(left == 0 || file_buffer_[current_buf_index_]->Full());
			if(file_buffer_[current_buf_index_]->Full()){
				current_buf_index_++;
			}
		}
    assert(left == 0);
		if(last_sync_index_ + 1 < current_buf_index_ ){
			last_sync_index_++;
			assert(file_buffer_[last_sync_index_]->synced_ == false);
      //auto start = SPDK_TIME;
			bool success = SPDKWrite(sp_info_, file_buffer_[last_sync_index_]);
			if(UNLIKELY(!success)){
				last_sync_index_--;
			}
      //std::this_thread::sleep_for(std::chrono::milliseconds(11));
      // while(!file_buffer_[last_sync_index_]->synced_){
      //   CheckComplete(sp_info_, file_buffer_[last_sync_index_]->queue_id_);
      // }
		}
		metadata_->modified_time_ = Now();
    metadata_->size_ += data.size();
		return Status::OK();
	}

	Status Fsync() {
		//write [last_sync_index_ + 1, current_buf_index_ - 1]
		while(last_sync_index_ + 1 < current_buf_index_){
			last_sync_index_++;
			assert(!file_buffer_[last_sync_index_]->synced_);
			while(!SPDKWrite(sp_info_, file_buffer_[last_sync_index_]))  ;
		}
    if(!file_buffer_[current_buf_index_]->Empty()){
      while(!SPDKWrite(sp_info_, file_buffer_[current_buf_index_])) ;
      last_sync_index_++;
    }

    #ifdef WAIT_CHECKING
      std::unique_lock<std::mutex> lk(pending_sync_mu_);
      pending_sync_cv_.wait(lk, [this]{return this->pending_sync_num_.load() == 0;});
    #else
      for(int64_t i=0; i<=last_sync_index_; i++){
        while(!file_buffer_[i]->synced_){
          int queue_id = file_buffer_[i]->queue_id_;
          CheckComplete(sp_info_, queue_id);
        }
      }
    #endif
		return Status::OK();
	}

	Status Close(){
    // for(uint64_t i=0; i<file_buffer_num_; i++){
    //   if(file_buffer_[i]->spdk_buffer_ != nullptr){
    //     file_buffer_[i]->FreeSPDKBuffer();
    //   }
		// }
    // printf("close spdk file: %s, cur_index: %ld, file_buffer_num: %ld\n", fname_.c_str(), current_buf_index_, file_buffer_num_);
		return Status::OK();
	}

  void Ref(){
    refs_++;
    //printf("%s unref:%" PRId64 "\n", fname_.c_str(), refs_);
    assert(refs_ > 0);
  }

  void Unref(){
    refs_--;
    //printf("%s unref:%" PRId64 "\n", fname_.c_str(), refs_);
    assert(refs_ >= 0);
    if(refs_ == 0){
      {
        MutexLock lock(&fs_mutex_);
        file_system_.erase(fname_);
      }
      //printf("delete %s\n", fname_.c_str());
      delete this;
    }
  }

	uint64_t ModifiedTime() const { return metadata_->modified_time_; }

  uint64_t LastAccessTime(){return metadata_->last_access_time_;}

  int64_t GetRef(){return refs_;}

 private:
	uint64_t Now() {
		int64_t unix_time = 0;
    	auto s = env_->GetCurrentTime(&unix_time);
    	assert(s.ok());
    	return static_cast<uint64_t>(unix_time);
	}

	Env* env_;
	FileBuffer **file_buffer_;
	const std::string fname_;
	mutable port::Mutex mutex_;
	bool locked_;
	Random rnd_;
	int64_t current_buf_index_;
	int64_t last_sync_index_;
  int64_t refs_;
  SPDKFileMeta *metadata_;
  uint64_t file_buffer_num_;
  std::atomic<uint64_t> pending_sync_num_;
  std::mutex pending_sync_mu_;
  std::condition_variable pending_sync_cv_;
};

static void EvictFileData(){
  assert(file_system_.size() > 0);
  uint64_t min = 1ul<<63;
  SpdkFile *file = nullptr;
  {
    std::string fname = "";
    MutexLock lock(&fs_mutex_);
    for(auto &it : file_system_){
      if(it.second->GetRef() == 1 &&
          it.second->LastAccessTime() < min){
        fname = it.first;
        file = it.second;
        min = it.second->LastAccessTime();
      }
    }
    if(UNLIKELY(fname == "")){
      fprintf(stderr, "EvictFileData() can not evict file\n");
      printf("Can not get buffer after delete data\n");
      printf("SPDK metadata size: %ld\n", file_meta_.size());
      printf("File system size: %ld\n", file_system_.size());

      printf("meta:\n");
      for(auto &meta : file_meta_){
        printf("fname: %s, size: %ld\n", meta.second->fname_.c_str(), meta.second->size_);
      }
      printf("file:\n");
      for(auto &f : file_system_){
        printf("fname: %s, ref: %ld\n", f.first.c_str(), f.second->GetRef());
      }
      ExitError();
    }
    printf("evict %s\n", fname.c_str());
    file_system_.erase(fname);//
  }
  assert(file != nullptr);
  file->Unref();
  // printf("unref %s\n", fname.c_str());
}

class SpdkRandomAccessFile : public RandomAccessFile {
 public:
  explicit SpdkRandomAccessFile(SpdkFile* file) : file_(file) { file_->Ref(); }

  ~SpdkRandomAccessFile() override { file_->Unref(); }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    return file_->Read(offset, n, result, scratch);
  }

 private:
  SpdkFile* file_;
};

class SpdkWritableFile : public WritableFile {
 public:
  SpdkWritableFile(SpdkFile* file, RateLimiter* rate_limiter)
      : file_(file),
        rate_limiter_(rate_limiter){ file_->Ref(); }

  ~SpdkWritableFile() override { file_->Unref(); }

  Status Append(const Slice& data) override {
    size_t bytes_written = 0;
    while (bytes_written < data.size()) {
      auto bytes = RequestToken(data.size() - bytes_written);
      Status s = file_->Append(Slice(data.data() + bytes_written, bytes));
      if (!s.ok()) {
        return s;
      }
      bytes_written += bytes;
    }
    return Status::OK();
  }

  Status Truncate(uint64_t size) override {
    file_->Truncate(static_cast<size_t>(size));
    return Status::OK();
  }
  Status Close() override { file_->Fsync(); return file_->Close(); }

  Status Flush() override { return Status::OK(); }

  Status Sync() override { return file_->Fsync(); }

  uint64_t GetFileSize() override { return file_->Size(); }

 private:
  inline size_t RequestToken(size_t bytes) {
    if (rate_limiter_ && io_priority_ < Env::IO_TOTAL) {
      bytes = std::min(
          bytes, static_cast<size_t>(rate_limiter_->GetSingleBurstBytes()));
      rate_limiter_->Request(bytes, io_priority_);
    }
    return bytes;
  }
  SpdkFile* file_;
  RateLimiter* rate_limiter_;
};

static void Exit(){
  #ifdef WAIT_CHECKING
    stop_.store(true);
    check_thread_.join();
  #endif
  //1.free meta and filesystem
  for(auto it=file_system_.begin(); it!=file_system_.end();){
    auto next = ++it;
    it--;
    assert(it->second != nullptr);
    it->second->Unref();
    it = next;
  }
  //2.write metadata and free meta buffer
  if(meta_buffer_ != nullptr){
    WriteMeta(&file_meta_);
    spdk_free(meta_buffer_);
  }
  //3.delete metadata
  for(auto &meta : file_meta_){
    assert(meta.second != nullptr);
    delete meta.second;
  }
  //4.free mem pool
  if(spdk_mem_pool_ != nullptr){
    //spdk_mem_pool_->stop();
    // SPDKBuffer *buf = spdk_mem_pool_->ReadValue();
    /*SPDKBuffer *buf = spdk_mem_pool_->Get();
    while(buf != nullptr){
      if(buf->buffer_ != nullptr)
        spdk_free(buf->buffer_);
      delete buf;
      // buf = spdk_mem_pool_->ReadValue();
      buf = spdk_mem_pool_->Get();
    }*/
    delete spdk_mem_pool_;
  }
  if(huge_pages_ != nullptr){
    delete huge_pages_;
  }
  //5.free mutex
  if(spdk_queue_mutexes_ != nullptr){
    for(int i=0; i<total_queue_num_; i++){
      delete spdk_queue_mutexes_[i];
    }
    delete spdk_queue_mutexes_;
  }
  //6. output
 #ifdef LOGSDB_STAT 
  printf("--------------L0 env---------------------\n");
  printf("FILE_BUFFER_ENTRY_SIZE: %.2lf MB\n", FILE_BUFFER_ENTRY_SIZE/(1024.0*1024.0));
  printf("write latency: %ld %.2lf us\n", write_latency_.Num(), write_latency_.Avg()/1000);
  printf("read latency: %ld %.2lf us\n", read_latency_.Num(), read_latency_.Avg()/1000);
  printf("read miss latency: %ld %.2lf us\n", read_miss_latency_.Num(), read_miss_latency_.Avg()/1000);
  if(read_hit_ + read_miss_ != 0){
    printf("read hit: %ld, read miss: %ld\n", read_hit_, read_miss_);
    printf("read hit ratio: %lf\n", (read_hit_ * 1.0)/(read_hit_ + read_miss_));
  }
  printf("------------------------------------------\n");
 #endif
}

static void check_overlap(){
  uint64_t occupy_page=0;
  for(auto &m : file_meta_){
    occupy_page+=(m.second->end_lpn_-m.second->start_lpn_);
  }
  printf("the occupied size is %ld GB\n",occupy_page*SPDK_PAGE_SIZE/1024/1024/1024);
  ExitError()
}

static void DumpL0(FileMeta *file_meta, std::string fname){
  printf("Dump SPDK....\n");
  // printf("outputname: %s\n", fname.c_str());
  WriteMeta(&file_meta_);
  int fd = open(fname.c_str(), O_WRONLY | O_CREAT);
  uint64_t buffer_size = META_SIZE;
  char *buffer = (char*)spdk_zmalloc(buffer_size, 
                                    SPDK_PAGE_SIZE, NULL, 
                                    SPDK_ENV_SOCKET_ID_ANY,
                                    SPDK_MALLOC_DMA);
  //1. write metadata
  assert(buffer_size == META_SIZE);
  SPDKReadSync(sp_info_, buffer, LO_START_LPN, buffer_size);
  write(fd, buffer, buffer_size);
  //printf("write metadata\n");
  //2. write data
  EncodeFixed32(buffer, (uint32_t)(file_meta->size()));
  write(fd, buffer, 4);
  buffer_size = SPDK_PAGE_SIZE;
  for(auto &meta : *file_meta){
    //printf("%s\n", meta.second->ToString().c_str());
    uint64_t start_lpn = meta.second->start_lpn_;
    uint64_t end_lpn = meta.second->end_lpn_;
    //printf("%ld %ld\n", start_lpn, end_lpn);
    EncodeFixed64(buffer, start_lpn);
    EncodeFixed64(buffer + 8, end_lpn);
    write(fd, buffer, 16);
    assert((end_lpn - start_lpn + 1) % (buffer_size/SPDK_PAGE_SIZE) == 0);
    uint64_t lpn = start_lpn;
    while(lpn < end_lpn){
      SPDKReadSync(sp_info_, buffer, lpn, buffer_size);
      write(fd, buffer, buffer_size);
      lpn += buffer_size/SPDK_PAGE_SIZE;
    }
  }
  close(fd);
  spdk_free(buffer);
  printf("Dump SPDK finished\n");
}

static void RecoveryL0(std::string fname){
  printf("Recovery SPDK...\n");
  //printf("outputname: %s\n", fname.c_str());
  int fd = open(fname.c_str(), O_RDONLY);
  uint64_t buffer_size = META_SIZE;
  char *buffer = (char*)spdk_zmalloc(buffer_size, 
                                    SPDK_PAGE_SIZE, NULL, 
                                    SPDK_ENV_SOCKET_ID_ANY,
                                    SPDK_MALLOC_DMA);
  //1. recovery metadata
  read(fd, buffer, buffer_size);
  SPDKWriteSync(sp_info_, buffer, LO_START_LPN, buffer_size);
  //2. recovery data
  read(fd, buffer, 4);
  uint32_t size = DecodeFixed32(buffer);
  // printf("recovery %d files from L0\n", size);
  buffer_size = SPDK_PAGE_SIZE;
  for(uint32_t i=0; i<size; i++){
    read(fd, buffer, 16);
    uint64_t start_lpn = DecodeFixed64(buffer);
    uint64_t end_lpn = DecodeFixed64(buffer+8);
    //printf("%d: %ld %ld\n", i, start_lpn, end_lpn);
    assert((end_lpn - start_lpn + 1) % (buffer_size/SPDK_PAGE_SIZE) == 0);
    uint64_t lpn = start_lpn;
    while(lpn < end_lpn){
      read(fd, buffer, buffer_size);
      SPDKWriteSync(sp_info_, buffer, lpn, buffer_size);
      lpn += buffer_size/SPDK_PAGE_SIZE;
    }
  }
  close(fd);
  spdk_free(buffer);
  printf("Recovery SPDK finished\n");
}

static uint64_t MoveToSPDK(std::string path, int max_level){
  int MAXLEVEL = 100;
  std::vector<std::vector<std::string>> files_level;
  files_level.reserve(MAXLEVEL);
  //1. get file
	FILE *inputfile = fopen((path + "/spdk.sst").c_str(), "r");
  if(inputfile == nullptr){
    fprintf(stderr, "Can not open file %s\n", (path + "/spdk.sst").c_str());
    ExitError();
  }
	char line[201];
	fgets(line,200,inputfile);
	while(!feof(inputfile)){
		int level;
		char fname[100];
		sscanf(line, "level is %d , fname is %s", &level, fname);
    // printf("level %d, file: %s\n", level, fname);
		files_level[level].push_back(std::string(fname));
		fgets(line,200,inputfile);
	}
	fclose(inputfile);
  //2. read from file, write to device
  uint64_t g_start_lpn = LO_START_LPN + META_SIZE/SPDK_PAGE_SIZE;
  uint64_t buffer_size = 1ull<<20;
  char *buffer = (char*)spdk_zmalloc(buffer_size, SPDK_PAGE_SIZE, NULL, 
                                    SPDK_ENV_SOCKET_ID_ANY,SPDK_MALLOC_DMA);
  for(int i=0; i<MAXLEVEL; i++){
    if(i > max_level)
      continue;
    printf("moving level %d...\n", i);
    for(auto &file : files_level[i]){
      // printf("moving file at level %d : %s\n", i, file.c_str());
      uint64_t start_lpn = g_start_lpn;
      uint64_t end_lpn = start_lpn;
      std::string fname = path + "/" + file;
      int fd = open(fname.c_str(), O_RDONLY);
      if(fd < 0){
        fprintf(stderr, "open %s failed\n", fname.c_str());
        ExitError();
      }
      ssize_t size = 0;
      uint64_t file_size = 0;
      while((size = read(fd, buffer, buffer_size)) > 0){
        file_size += size;
        if(size % SPDK_PAGE_SIZE != 0){
          size = (size / SPDK_PAGE_SIZE + 1) * SPDK_PAGE_SIZE;
        }
        uint64_t page_num = size / SPDK_PAGE_SIZE;
        SPDKWriteSync(sp_info_, buffer, end_lpn, size);
        end_lpn += page_num;
      }
      if(size < 0){
        fprintf(stderr, "read %s failed : %s\n", file.c_str(), strerror(errno));
      }
      close(fd);
      //system(("rm -f " + fname).c_str()); // delete from linux file system
      SPDKFileMeta* meta= new SPDKFileMeta(fname, start_lpn, end_lpn-1, false, file_size);
      file_meta_[fname]= meta;
      g_start_lpn = end_lpn;
      assert(g_start_lpn < L0_MAX_LPN);
    }
  }
  spdk_free(buffer);
  fflush(stdout);
  return g_start_lpn;
}

}