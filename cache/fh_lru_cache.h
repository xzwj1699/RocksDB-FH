//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <memory>
#include <string>

#include "cache/sharded_cache.h"
#include "port/lang.h"
#include "port/likely.h"
#include "port/malloc.h"
#include "port/port.h"
#include "util/autovector.h"
#include "util/distributed_mutex.h"

#include <atomic>
#include <queue>
#include <chrono>
#include <thread>
#include <iostream>

#define GRANULARITY 100

namespace ROCKSDB_NAMESPACE {
namespace fh_lru_cache {

// Learning_Result_Node and Learning_Input_Node are used to decide 
// FH Cache construct ratio
struct Learning_Result_Node {
  double ratio_, metrics_, granularity_;
  bool status_;
  Learning_Result_Node(double ratio = 0, double metrics = 0, \
    double granularity = 0, bool status = false):
    ratio_(ratio), metrics_(metrics), granularity_(granularity), status_(status) {}
};

struct Learning_Input_Node {
  double ratio_, granularity_;
  Learning_Input_Node(double ratio = 0, double granularity = 0):
    ratio_(ratio), granularity_(granularity) {}
};

struct FH_cmp {
  bool operator()(Learning_Result_Node a, Learning_Result_Node b) {
    return a.metrics_ < b.metrics_;
  }
};

// LRU cache implementation. This class is not thread-safe.

// An entry is a variable length heap-allocated structure.
// Entries are referenced by cache and/or by any external entity.
// The cache keeps all its entries in a hash table. Some elements
// are also stored on LRU list.
//
// FHLRUHandle can be in these states:
// 1. Referenced externally AND in hash table.
//    In that case the entry is *not* in the LRU list
//    (refs >= 1 && in_cache == true)
// 2. Not referenced externally AND in hash table.
//    In that case the entry is in the LRU list and can be freed.
//    (refs == 0 && in_cache == true)
// 3. Referenced externally AND not in hash table.
//    In that case the entry is not in the LRU list and not in hash table.
//    The entry must be freed if refs becomes 0 in this state.
//    (refs >= 1 && in_cache == false)
// If you call LRUCacheShard::Release enough times on an entry in state 1, it
// will go into state 2. To move from state 1 to state 3, either call
// LRUCacheShard::Erase or LRUCacheShard::Insert with the same key (but
// possibly different value). To move from state 2 to state 1, use
// LRUCacheShard::Lookup.
// While refs > 0, public properties like value and deleter must not change.

struct FHLRUHandle {
  Cache::ObjectPtr value;
  const Cache::CacheItemHelper* helper;
  FHLRUHandle* next_hash;
  // Pointer to next FHLRUHandle in FH hashtable (fast hashtable)
  FHLRUHandle* next_fh_hash;
  FHLRUHandle* next;
  FHLRUHandle* prev;
  size_t total_charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  // The hash of key(). Used for fast sharding and comparisons.
  uint32_t hash;
  // The number of external refs to this entry. The cache itself is not counted.
  uint32_t refs;

  // Mutable flags - access controlled by mutex
  // The m_ and M_ prefixes (and im_ and IM_ later) are to hopefully avoid
  // checking an M_ flag on im_flags or an IM_ flag on m_flags.
  uint8_t m_flags;
  enum MFlags : uint8_t {
    // Whether this entry is referenced by the hash table.
    M_IN_CACHE = (1 << 0),
    // Whether this entry has had any lookups (hits).
    M_HAS_HIT = (1 << 1),
    // Whether this entry is in high-pri pool.
    M_IN_HIGH_PRI_POOL = (1 << 2),
    // Whether this entry is in low-pri pool.
    M_IN_LOW_PRI_POOL = (1 << 3),
    // Whether this entry is in Frozen Part of FH Cache
    M_IN_FH = (1 << 4),
    // Whether this entry is a tomb (happened in user delete or LSM-tree compaction)
    M_IS_TOMB = (1 << 5)
  };

  // "Immutable" flags - only set in single-threaded context and then
  // can be accessed without mutex
  uint8_t im_flags;
  enum ImFlags : uint8_t {
    // Whether this entry is high priority entry.
    IM_IS_HIGH_PRI = (1 << 0),
    // Whether this entry is low priority entry.
    IM_IS_LOW_PRI = (1 << 1),
    // Marks result handles that should not be inserted into cache
    IM_IS_STANDALONE = (1 << 2),
  };

  // Beginning of the key (MUST BE THE LAST FIELD IN THIS STRUCT!)
  char key_data[1];

  Slice key() const { return Slice(key_data, key_length); }

  // For HandleImpl concept
  uint32_t GetHash() const { return hash; }

  // Increase the reference count by 1.
  void Ref() { refs++; }

  // Just reduce the reference count by 1. Return true if it was last reference.
  bool Unref() {
    if (refs == 0) {
      std::cout << "refs: " << refs << " in fh: " << InFH() << " in cache: " << InCache() << std::endl;
    }
    assert(refs > 0 || InFH());
    if (InFH() && refs == 0) return true;
    refs--;
    return refs == 0;
  }

  // Return true if there are external refs, false otherwise.
  bool HasRefs() const { return refs > 0; }

  bool InCache() const { return m_flags & M_IN_CACHE; }
  bool IsHighPri() const { return im_flags & IM_IS_HIGH_PRI; }
  bool InHighPriPool() const { return m_flags & M_IN_HIGH_PRI_POOL; }
  bool IsLowPri() const { return im_flags & IM_IS_LOW_PRI; }
  bool InLowPriPool() const { return m_flags & M_IN_LOW_PRI_POOL; }
  bool HasHit() const { return m_flags & M_HAS_HIT; }
  bool IsStandalone() const { return im_flags & IM_IS_STANDALONE; }

  /** Start of FronzenHot specific design **/

  bool InFH() const { return m_flags & M_IN_FH; }
  bool IsTomb() const { return m_flags & M_IS_TOMB; }

  /** End of FronzenHot specific design **/

  void SetInCache(bool in_cache) {
    if (in_cache) {
      m_flags |= M_IN_CACHE;
    } else {
      m_flags &= ~M_IN_CACHE;
    }
  }

  void SetPriority(Cache::Priority priority) {
    if (priority == Cache::Priority::HIGH) {
      im_flags |= IM_IS_HIGH_PRI;
      im_flags &= ~IM_IS_LOW_PRI;
    } else if (priority == Cache::Priority::LOW) {
      im_flags &= ~IM_IS_HIGH_PRI;
      im_flags |= IM_IS_LOW_PRI;
    } else {
      im_flags &= ~IM_IS_HIGH_PRI;
      im_flags &= ~IM_IS_LOW_PRI;
    }
  }

  void SetInHighPriPool(bool in_high_pri_pool) {
    if (in_high_pri_pool) {
      m_flags |= M_IN_HIGH_PRI_POOL;
    } else {
      m_flags &= ~M_IN_HIGH_PRI_POOL;
    }
  }

  void SetInLowPriPool(bool in_low_pri_pool) {
    if (in_low_pri_pool) {
      m_flags |= M_IN_LOW_PRI_POOL;
    } else {
      m_flags &= ~M_IN_LOW_PRI_POOL;
    }
  }

  void SetHit() { m_flags |= M_HAS_HIT; }

  void SetIsStandalone(bool is_standalone) {
    if (is_standalone) {
      im_flags |= IM_IS_STANDALONE;
    } else {
      im_flags &= ~IM_IS_STANDALONE;
    }
  }

  /** Start of FronzenHot specific design **/

  void SetInFH(bool in_fh) {
    if (in_fh) {
      m_flags |= M_IN_FH;
    } else {
      m_flags &= ~M_IN_FH;
    }
  }

  void SetIsTomb(bool is_tomb) {
    if (is_tomb) {
      m_flags |= M_IS_TOMB;
    } else {
      m_flags &= ~M_IS_TOMB;
    }
  }

  /** End of FronzenHot specific design **/

  void Free(MemoryAllocator* allocator) {
    assert(refs == 0);
    assert(helper);
    if (helper->del_cb) {
      helper->del_cb(value, allocator);
    }

    free(this);
  }

  inline size_t CalcuMetaCharge(
      CacheMetadataChargePolicy metadata_charge_policy) const {
    if (metadata_charge_policy != kFullChargeCacheMetadata) {
      return 0;
    } else {
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      return malloc_usable_size(
          const_cast<void*>(static_cast<const void*>(this)));
#else
      // This is the size that is used when a new handle is created.
      return sizeof(FHLRUHandle) - 1 + key_length;
#endif
    }
  }

  // Calculate the memory usage by metadata.
  inline void CalcTotalCharge(
      size_t charge, CacheMetadataChargePolicy metadata_charge_policy) {
    total_charge = charge + CalcuMetaCharge(metadata_charge_policy);
  }

  inline size_t GetCharge(
      CacheMetadataChargePolicy metadata_charge_policy) const {
    size_t meta_charge = CalcuMetaCharge(metadata_charge_policy);
    assert(total_charge >= meta_charge);
    return total_charge - meta_charge;
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class FHLRUHandleTable {
 public:
  explicit FHLRUHandleTable(int max_upper_hash_bits, MemoryAllocator* allocator);
  ~FHLRUHandleTable();

  // These three functions are designed for FH Cache
  void Delete_Init();
  FHLRUHandle* FH_Lookup(const Slice& key, uint32_t hash);
  FHLRUHandle* FH_Insert(FHLRUHandle* h);

  FHLRUHandle* Lookup(const Slice& key, uint32_t hash);
  FHLRUHandle* Insert(FHLRUHandle* h);
  FHLRUHandle* Remove(const Slice& key, uint32_t hash);

  template <typename T>
  void ApplyToEntriesRange(T func, size_t index_begin, size_t index_end) {
    for (size_t i = index_begin; i < index_end; i++) {
      FHLRUHandle* h = list_[i];
      while (h != nullptr) {
        auto n = h->next_hash;
        assert(h->InCache());
        func(h);
        h = n;
      }
    }
  }

  int GetLengthBits() const { return length_bits_; }

  size_t GetOccupancyCount() const { return elems_; }

  MemoryAllocator* GetAllocator() const { return allocator_; }

 private:
  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  FHLRUHandle** FindPointer(const Slice& key, uint32_t hash);

  // Almost the same as the above find-pointer function, except that
  // use fh_next_hash as next pointer
  FHLRUHandle** FH_FindPointer(const Slice& key, uint32_t hash);

  void Resize();

  // Resize function used in FH Cache
  void FH_Resize();

  // Number of hash bits (upper because lower bits used for sharding)
  // used for table index. Length == 1 << length_bits_
  int length_bits_;

  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  std::unique_ptr<FHLRUHandle*[]> list_;

  // Number of elements currently in the table.
  uint32_t elems_;

  // Set from max_upper_hash_bits (see constructor).
  const int max_length_bits_;

  // From Cache, needed for delete
  MemoryAllocator* const allocator_;
};

// A single shard of sharded cache.
class ALIGN_AS(CACHE_LINE_SIZE) FHLRUCacheShard final : public CacheShardBase {
 public:
  // NOTE: the eviction_callback ptr is saved, as is it assumed to be kept
  // alive in Cache.
  FHLRUCacheShard(size_t capacity, bool strict_capacity_limit,
                double high_pri_pool_ratio, double low_pri_pool_ratio,
                bool use_adaptive_mutex,
                CacheMetadataChargePolicy metadata_charge_policy,
                int max_upper_hash_bits, MemoryAllocator* allocator,
                const Cache::EvictionCallback* eviction_callback);

 public:  // Type definitions expected as parameter to ShardedCache
  using HandleImpl = FHLRUHandle;
  using HashVal = uint32_t;
  using HashCref = uint32_t;

 public:  // Function definitions expected as parameter to ShardedCache
  static inline HashVal ComputeHash(const Slice& key, uint32_t seed) {
    return Lower32of64(GetSliceNPHash64(key, seed));
  }

  // Separate from constructor so caller can easily make an array of LRUCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space.
  void SetCapacity(size_t capacity);

  // Set the flag to reject insertion if cache if full.
  void SetStrictCapacityLimit(bool strict_capacity_limit);

  // Set percentage of capacity reserved for high-pri cache entries.
  void SetHighPriorityPoolRatio(double high_pri_pool_ratio);

  // Set percentage of capacity reserved for low-pri cache entries.
  void SetLowPriorityPoolRatio(double low_pri_pool_ratio);

  // Like Cache methods, but with an extra "hash" parameter.
  Status Insert(const Slice& key, uint32_t hash, Cache::ObjectPtr value,
                const Cache::CacheItemHelper* helper, size_t charge,
                FHLRUHandle** handle, Cache::Priority priority);

  FHLRUHandle* CreateStandalone(const Slice& key, uint32_t hash,
                              Cache::ObjectPtr obj,
                              const Cache::CacheItemHelper* helper,
                              size_t charge, bool allow_uncharged);

  FHLRUHandle* Lookup(const Slice& key, uint32_t hash,
                    const Cache::CacheItemHelper* helper,
                    Cache::CreateContext* create_context,
                    Cache::Priority priority, Statistics* stats);

  bool Release(FHLRUHandle* handle, bool useful, bool erase_if_last_ref);
  bool Ref(FHLRUHandle* handle);
  void Erase(const Slice& key, uint32_t hash);

  // Although in some platforms the update of size_t is atomic, to make sure
  // GetUsage() and GetPinnedUsage() work correctly under any platform, we'll
  // protect them with mutex_.

  size_t GetUsage() const;
  size_t GetPinnedUsage() const;
  size_t GetOccupancyCount() const;
  size_t GetTableAddressCount() const;
  size_t GetLRUelems() const;

  void ApplyToSomeEntries(
      const std::function<void(const Slice& key, Cache::ObjectPtr value,
                               size_t charge,
                               const Cache::CacheItemHelper* helper)>& callback,
      size_t average_entries_per_lock, size_t* state);

  void EraseUnRefEntries();

 public:  // other function definitions
  void TEST_GetLRUList(FHLRUHandle** lru, FHLRUHandle** lru_low_pri,
                       FHLRUHandle** lru_bottom_pri);

  // Retrieves number of elements in LRU, for unit test purpose only.
  // Not threadsafe.
  size_t TEST_GetLRUSize();

  // Retrieves high pri pool ratio
  double GetHighPriPoolRatio();

  // Retrieves low pri pool ratio
  double GetLowPriPoolRatio();

  void AppendPrintableOptions(std::string& /*str*/) const;

 private:
  /** Start of FrozenHot specific design **/

  // Insert a dummy marker at the beginning of LRU list
  bool InsertMarker();

  // Find the before inserted marker, return false if not found
  bool FindMarker();

  // Construct FH Cache from the marker
  bool ConstructFromMarker();

  // Construct FH Cache from a specific ratio
  bool ConstructFromRatio(double ratio);

  // Deconstruct the FH cache
  void Deconstruct();

  // A helper function for debug
  void FHDebug(FHLRUHandle* e, int flag);

  // Learning function in FH Algorithm
  Learning_Input_Node learning_machine(Learning_Input_Node last_sleep_node, double last_iops);

  /** End of FrozenHot specific design **/

 public:
  // Statistics function for FH
  inline double _get_miss_ratio();
  inline double _get_reset_miss_ratio();
  inline double _get_FH_miss_ratio();
  inline double _print_reset_FH();
  inline double _get_FH();
  inline double _print_FH();
  inline double _print_iops_FH();
  inline void _reset_FH();
  inline void _print_usage();
  inline void _print_usage_wlock();

 private:
  friend class FHLRUCache;
  // Insert an item into the hash table and, if handle is null, insert into
  // the LRU list. Older items are evicted as necessary. Frees `item` on
  // non-OK status.
  Status InsertItem(FHLRUHandle* item, FHLRUHandle** handle);

  void LRU_Remove(FHLRUHandle* e);
  void LRU_Insert(FHLRUHandle* e);

  // Overflow the last entry in high-pri pool to low-pri pool until size of
  // high-pri pool is no larger than the size specify by high_pri_pool_pct.
  void MaintainPoolSize();

  // Free some space following strict LRU policy until enough space
  // to hold (usage_ + charge) is freed or the lru list is empty
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_.
  void EvictFromLRU(size_t charge, autovector<FHLRUHandle*>* deleted);

  void NotifyEvicted(const autovector<FHLRUHandle*>& evicted_handles);

  FHLRUHandle* CreateHandle(const Slice& key, uint32_t hash,
                          Cache::ObjectPtr value,
                          const Cache::CacheItemHelper* helper, size_t charge);

  // Random function for FH Cache statistics, return true in ~ 1/Granularity 
  // possibility, Granularity is now an ad-hot super-param
  bool RandomSample();
  
  // Initialized before use.
  size_t capacity_;

  // Memory size for entries in high-pri pool.
  size_t high_pri_pool_usage_;

  // Memory size for entries in low-pri pool.
  size_t low_pri_pool_usage_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  // Ratio of capacity reserved for high priority cache entries.
  double high_pri_pool_ratio_;

  // High-pri pool size, equals to capacity * high_pri_pool_ratio.
  // Remember the value to avoid recomputing each time.
  double high_pri_pool_capacity_;

  // Ratio of capacity reserved for low priority cache entries.
  double low_pri_pool_ratio_;

  // Low-pri pool size, equals to capacity * low_pri_pool_ratio.
  // Remember the value to avoid recomputing each time.
  double low_pri_pool_capacity_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // LRU contains items which can be evicted, ie reference only by cache
  FHLRUHandle lru_;

  // Pointer to head of low-pri pool in LRU list.
  FHLRUHandle* lru_low_pri_;

  // Pointer to head of bottom-pri pool in LRU list.
  FHLRUHandle* lru_bottom_pri_;

  // Whether FH Cache construction is ready
  bool FH_ready = false;

  // Dummy head of FH LRU list.
  FHLRUHandle fh_lru_;

  // Some learning machine related data
  int COUNT_THRESHOLD = 4;
  int miss_ratio_count = COUNT_THRESHOLD;
  double TUNING_STABLE_THRESHOLD = 0.0003;
  double TUNING_STABLE_STEP = 1;
  double last_sleep = 0;
  double EPSILON = 0.0001;
  double best_sleep = 0;
  double best_miss = 1;
  std::queue<Learning_Input_Node> ratio_container;
  std::priority_queue<Learning_Result_Node, std::vector<Learning_Result_Node>, FH_cmp> learning_container;

  // ------------^^^^^^^^^^^^^-----------
  // Not frequently modified data members
  // ------------------------------------
  //
  // We separate data members that are updated frequently from the ones that
  // are not frequently updated so that they don't share the same cache line
  // which will lead into false cache sharing
  //
  // ------------------------------------
  // Frequently modified data members
  // ------------vvvvvvvvvvvvv-----------
  FHLRUHandleTable table_;

  // Faster hashtable for FH Cache
  FHLRUHandleTable FH_table_;

  // Memory size for entries residing in the cache.
  size_t usage_;

  // Memory size for entries residing only in the LRU list.
  size_t lru_usage_;

  // LRU elements number for entries residing only in the LRU list
  size_t lru_elems_;

  // Total lookup success count
  size_t _lookup_succ = 0;

  // FH lookup success count
  size_t _fh_lookup_succ = 0;

  // Total lookup fail count
  size_t _lookup_fail = 0;

  // Handled requests count
  std::atomic<size_t> handle_req_num = 0;

  // mutex_ protects the following state.
  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable DMutex mutex_;

  // A reference to Cache::eviction_callback_
  const Cache::EvictionCallback& eviction_callback_;
};

class FHLRUCache
#ifdef NDEBUG
    final
#endif
    : public ShardedCache<FHLRUCacheShard> {
 public:
  explicit FHLRUCache(const FHLRUCacheOptions& opts);
  ~FHLRUCache();
  const char* Name() const override { return "FHLRUCache"; }
  ObjectPtr Value(Handle* handle) override;
  size_t GetCharge(Handle* handle) const override;
  const CacheItemHelper* GetCacheItemHelper(Handle* handle) const override;

  // Retrieves number of elements in LRU, for unit test purpose only.
  size_t TEST_GetLRUSize();
  // Retrieves high pri pool ratio.
  double GetHighPriPoolRatio();

  void FH_Set_Status(bool status);

  bool FH_Get_Status();

  void FH_Scheduler();

 private:
  /** Start of FrozenHot specific design **/

  std::vector<std::thread> thd_vec;
  
  // Indicate whether cache is in Frozen status
  bool FH_status = true;

  // TODO: ??
  bool FH_ready_record_reset_flag[100] = {false};

  // TODO: ??
  size_t total_capacity;

  // TODO: ??
  int LARGE_GRANULARITY = 4000;

  // TODO: ??
  double WAIT_STABLE_THRESHOLD = 0.005;

  // TODO: ??
  int WAIT_STABLE_SLEEP_INTERVAL_US = 1e6; // 1s

  // TODO: ??
  int WAIT_MARKER_SLEEP_INTERVAL_US = 1e6; // 1s

  // TODO: ??
  double QUERY_INTERVAL_US = 5e5; // 0.5s

  // TODO: ??
  double WAIT_STABLE_QUERY_INTERVAL_US = 5e6; // 5s

  // TODO: ??
  double WAIT_DYNAMIC_SLEEP_INTERVAL_US = 3e6; // 3s

  // TODO: ??
  double QUERY_STABLE_THRESHOLD = 0.03;

  // TODO: ??
  std::queue<int> construct_container;

  /** End of FrozenHot specific design **/
};

}  // namespace fh_lru_cache

using FHLRUCache = fh_lru_cache::FHLRUCache;
using FHLRUHandle = fh_lru_cache::FHLRUHandle;
using FHLRUCacheShard = fh_lru_cache::FHLRUCacheShard;

}  // namespace ROCKSDB_NAMESPACE
