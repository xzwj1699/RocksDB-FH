//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/fh_lru_cache.h"
#include <bits/stdint-uintn.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <ratio>
#include <thread>
#include <utility>

#include "cache/secondary_cache_adapter.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "port/lang.h"
#include "util/distributed_mutex.h"

namespace ROCKSDB_NAMESPACE {
namespace fh_lru_cache {

// Whether count handle_req_num
bool status_iops = false;

FHLRUHandleTable::FHLRUHandleTable(int max_upper_hash_bits,
                               MemoryAllocator* allocator)
    : length_bits_(/* historical starting size*/ 4),
      list_(new FHLRUHandle* [size_t{1} << length_bits_] {}),
      elems_(0),
      max_length_bits_(max_upper_hash_bits),
      allocator_(allocator) {
        // std::cout << "RocksbDB LRU Cache init!" << std::endl;
      }

FHLRUHandleTable::~FHLRUHandleTable() {
  auto alloc = allocator_;
  ApplyToEntriesRange(
      [alloc](FHLRUHandle* h) {
        if (!h->HasRefs()) {
          h->Free(alloc);
        }
      },
      0, size_t{1} << length_bits_);
}

void FHLRUHandleTable::Delete_Init() {
  length_bits_ = 4;
  std::unique_ptr<FHLRUHandle* []> new_list {
    new FHLRUHandle* [size_t{1} << length_bits_] {}
  };
  list_ = std::move(new_list);
  elems_ = 0;
  // std::cout << "Before delete init FH resize" << std::endl;
  FH_Resize();
  // std::cout << "After delete init FH resize" << std::endl;
}

FHLRUHandle* FHLRUHandleTable::FH_Lookup(const Slice& key, uint32_t hash) {
  return *FH_FindPointer(key, hash);
}

FHLRUHandle* FHLRUHandleTable::FH_Insert(FHLRUHandle* h) {
  FHLRUHandle** ptr = FH_FindPointer(h->key(), h->hash);
  FHLRUHandle* old = *ptr;
  h->next_fh_hash = (old == nullptr ? nullptr : old->next_fh_hash);
  *ptr = h;
  if (old == nullptr) {
    ++elems_;
    if ((elems_ >> length_bits_) > 0) {  // elems_ >= length
      // Since each cache entry is fairly large, we aim for a small
      // average linked list length (<= 1).
      FH_Resize();
    }
  }
  return old;
}

FHLRUHandle** FHLRUHandleTable::FH_FindPointer(const Slice& key, uint32_t hash) {
  FHLRUHandle** ptr = &list_[hash >> (32 - length_bits_)];
  while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_fh_hash;
  }
  return ptr;
}

void FHLRUHandleTable::FH_Resize() {
  if (length_bits_ >= max_length_bits_) {
    // Due to reaching limit of hash information, if we made the table bigger,
    // we would allocate more addresses but only the same number would be used.
    return;
  }
  if (length_bits_ >= 31) {
    // Avoid undefined behavior shifting uint32_t by 32.
    return;
  }

  uint32_t old_length = uint32_t{1} << length_bits_;
  int new_length_bits = length_bits_ + 1;
  std::unique_ptr<FHLRUHandle* []> new_list {
    new FHLRUHandle* [size_t{1} << new_length_bits] {}
  };
  [[maybe_unused]] uint32_t count = 0;
  for (uint32_t i = 0; i < old_length; i++) {
    FHLRUHandle* h = list_[i];
    while (h != nullptr) {
      FHLRUHandle* next = h->next_fh_hash;
      uint32_t hash = h->hash;
      FHLRUHandle** ptr = &new_list[hash >> (32 - new_length_bits)];
      h->next_fh_hash = *ptr;
      *ptr = h;
      h = next;
      count++;
    }
  }
  // std::cout << "elems_: " << elems_ << " count: " << count << std::endl;
  assert(elems_ == count);
  list_ = std::move(new_list);
  length_bits_ = new_length_bits;
}

FHLRUHandle* FHLRUHandleTable::Lookup(const Slice& key, uint32_t hash) {
  return *FindPointer(key, hash);
}

FHLRUHandle* FHLRUHandleTable::Insert(FHLRUHandle* h) {
  FHLRUHandle** ptr = FindPointer(h->key(), h->hash);
  FHLRUHandle* old = *ptr;
  h->next_hash = (old == nullptr ? nullptr : old->next_hash);
  *ptr = h;
  if (old == nullptr) {
    ++elems_;
    if ((elems_ >> length_bits_) > 0) {  // elems_ >= length
      // Since each cache entry is fairly large, we aim for a small
      // average linked list length (<= 1).
      Resize();
    }
  }
  return old;
}

FHLRUHandle* FHLRUHandleTable::Remove(const Slice& key, uint32_t hash) {
  FHLRUHandle** ptr = FindPointer(key, hash);
  FHLRUHandle* result = *ptr;
  if (result != nullptr) {
    *ptr = result->next_hash;
    --elems_;
  }
  return result;
}

FHLRUHandle** FHLRUHandleTable::FindPointer(const Slice& key, uint32_t hash) {
  FHLRUHandle** ptr = &list_[hash >> (32 - length_bits_)];
  while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

void FHLRUHandleTable::Resize() {
  if (length_bits_ >= max_length_bits_) {
    // Due to reaching limit of hash information, if we made the table bigger,
    // we would allocate more addresses but only the same number would be used.
    return;
  }
  if (length_bits_ >= 31) {
    // Avoid undefined behavior shifting uint32_t by 32.
    return;
  }

  uint32_t old_length = uint32_t{1} << length_bits_;
  int new_length_bits = length_bits_ + 1;
  std::unique_ptr<FHLRUHandle* []> new_list {
    new FHLRUHandle* [size_t{1} << new_length_bits] {}
  };
  [[maybe_unused]] uint32_t count = 0;
  for (uint32_t i = 0; i < old_length; i++) {
    FHLRUHandle* h = list_[i];
    while (h != nullptr) {
      FHLRUHandle* next = h->next_hash;
      uint32_t hash = h->hash;
      FHLRUHandle** ptr = &new_list[hash >> (32 - new_length_bits)];
      h->next_hash = *ptr;
      *ptr = h;
      h = next;
      count++;
    }
  }
  assert(elems_ == count);
  list_ = std::move(new_list);
  length_bits_ = new_length_bits;
}

FHLRUCacheShard::FHLRUCacheShard(size_t capacity, bool strict_capacity_limit,
                             double high_pri_pool_ratio,
                             double low_pri_pool_ratio, bool use_adaptive_mutex,
                             CacheMetadataChargePolicy metadata_charge_policy,
                             int max_upper_hash_bits,
                             MemoryAllocator* allocator,
                             const Cache::EvictionCallback* eviction_callback)
    : CacheShardBase(metadata_charge_policy),
      capacity_(0),
      high_pri_pool_usage_(0),
      low_pri_pool_usage_(0),
      strict_capacity_limit_(strict_capacity_limit),
      high_pri_pool_ratio_(high_pri_pool_ratio),
      high_pri_pool_capacity_(0),
      low_pri_pool_ratio_(low_pri_pool_ratio),
      low_pri_pool_capacity_(0),
      table_(max_upper_hash_bits, allocator),
      FH_table_(max_upper_hash_bits, allocator),
      usage_(0),
      lru_usage_(0),
      mutex_(use_adaptive_mutex),
      eviction_callback_(*eviction_callback) {
  // Make empty circular linked list.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  lru_low_pri_ = &lru_;
  lru_bottom_pri_ = &lru_;
  fh_lru_.next = &fh_lru_;
  fh_lru_.prev = &fh_lru_;
  // printf("initialize FHLRU cache with capacity: %ld\n", capacity);
  SetCapacity(capacity);
  // printf("FHLRUCacheShard init\n");
}

void FHLRUCacheShard::EraseUnRefEntries() {
  autovector<FHLRUHandle*> last_reference_list;
  {
    DMutexLock l(mutex_);
    while (lru_.next != &lru_) {
      FHLRUHandle* old = lru_.next;
      // LRU list contains only elements which can be evicted.
      assert(old->InCache() && !old->HasRefs());
      LRU_Remove(old);
      table_.Remove(old->key(), old->hash);
      old->SetInCache(false);
      assert(usage_ >= old->total_charge);
      usage_ -= old->total_charge;
      last_reference_list.push_back(old);
    }
  }

  for (auto entry : last_reference_list) {
    entry->Free(table_.GetAllocator());
  }
}

void FHLRUCacheShard::ApplyToSomeEntries(
    const std::function<void(const Slice& key, Cache::ObjectPtr value,
                             size_t charge,
                             const Cache::CacheItemHelper* helper)>& callback,
    size_t average_entries_per_lock, size_t* state) {
  // The state is essentially going to be the starting hash, which works
  // nicely even if we resize between calls because we use upper-most
  // hash bits for table indexes.
  DMutexLock l(mutex_);
  int length_bits = table_.GetLengthBits();
  size_t length = size_t{1} << length_bits;

  assert(average_entries_per_lock > 0);
  // Assuming we are called with same average_entries_per_lock repeatedly,
  // this simplifies some logic (index_end will not overflow).
  assert(average_entries_per_lock < length || *state == 0);

  size_t index_begin = *state >> (sizeof(size_t) * 8u - length_bits);
  size_t index_end = index_begin + average_entries_per_lock;
  if (index_end >= length) {
    // Going to end
    index_end = length;
    *state = SIZE_MAX;
  } else {
    *state = index_end << (sizeof(size_t) * 8u - length_bits);
  }

  table_.ApplyToEntriesRange(
      [callback,
       metadata_charge_policy = metadata_charge_policy_](FHLRUHandle* h) {
        callback(h->key(), h->value, h->GetCharge(metadata_charge_policy),
                 h->helper);
      },
      index_begin, index_end);
}

void FHLRUCacheShard::TEST_GetLRUList(FHLRUHandle** lru, FHLRUHandle** lru_low_pri,
                                    FHLRUHandle** lru_bottom_pri) {
  DMutexLock l(mutex_);
  *lru = &lru_;
  *lru_low_pri = lru_low_pri_;
  *lru_bottom_pri = lru_bottom_pri_;
}

size_t FHLRUCacheShard::TEST_GetLRUSize() {
  DMutexLock l(mutex_);
  FHLRUHandle* lru_handle = lru_.next;
  size_t lru_size = 0;
  while (lru_handle != &lru_) {
    lru_size++;
    lru_handle = lru_handle->next;
  }
  return lru_size;
}

double FHLRUCacheShard::GetHighPriPoolRatio() {
  DMutexLock l(mutex_);
  return high_pri_pool_ratio_;
}

double FHLRUCacheShard::GetLowPriPoolRatio() {
  DMutexLock l(mutex_);
  return low_pri_pool_ratio_;
}

void FHLRUCacheShard::LRU_Remove(FHLRUHandle* e) {
  assert(e->next != nullptr);
  assert(e->prev != nullptr);
  if (lru_low_pri_ == e) {
    lru_low_pri_ = e->prev;
  }
  if (lru_bottom_pri_ == e) {
    lru_bottom_pri_ = e->prev;
  }
  e->next->prev = e->prev;
  e->prev->next = e->next;
  e->prev = e->next = nullptr;
  assert(lru_usage_ >= e->total_charge);
  lru_usage_ -= e->total_charge;
  assert(!e->InHighPriPool() || !e->InLowPriPool());
  if (e->InHighPriPool()) {
    assert(high_pri_pool_usage_ >= e->total_charge);
    high_pri_pool_usage_ -= e->total_charge;
  } else if (e->InLowPriPool()) {
    assert(low_pri_pool_usage_ >= e->total_charge);
    low_pri_pool_usage_ -= e->total_charge;
  }
  lru_elems_ -= 1;
}

void FHLRUCacheShard::LRU_Insert(FHLRUHandle* e) {
  assert(e->next == nullptr);
  assert(e->prev == nullptr);
  if (high_pri_pool_ratio_ > 0 && (e->IsHighPri() || e->HasHit())) {
    // Inset "e" to head of LRU list.
    e->next = &lru_;
    e->prev = lru_.prev;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(true);
    e->SetInLowPriPool(false);
    high_pri_pool_usage_ += e->total_charge;
    MaintainPoolSize();
  } else if (low_pri_pool_ratio_ > 0 &&
             (e->IsHighPri() || e->IsLowPri() || e->HasHit())) {
    // Insert "e" to the head of low-pri pool.
    e->next = lru_low_pri_->next;
    e->prev = lru_low_pri_;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(false);
    e->SetInLowPriPool(true);
    low_pri_pool_usage_ += e->total_charge;
    MaintainPoolSize();
    lru_low_pri_ = e;
  } else {
    // Insert "e" to the head of bottom-pri pool.
    e->next = lru_bottom_pri_->next;
    e->prev = lru_bottom_pri_;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(false);
    e->SetInLowPriPool(false);
    // if the low-pri pool is empty, lru_low_pri_ also needs to be updated.
    if (lru_bottom_pri_ == lru_low_pri_) {
      lru_low_pri_ = e;
    }
    lru_bottom_pri_ = e;
  }
  lru_usage_ += e->total_charge;
  lru_elems_ += 1;
}

void FHLRUCacheShard::MaintainPoolSize() {
  while (high_pri_pool_usage_ > high_pri_pool_capacity_) {
    // Overflow last entry in high-pri pool to low-pri pool.
    lru_low_pri_ = lru_low_pri_->next;
    assert(lru_low_pri_ != &lru_);
    lru_low_pri_->SetInHighPriPool(false);
    lru_low_pri_->SetInLowPriPool(true);
    assert(high_pri_pool_usage_ >= lru_low_pri_->total_charge);
    high_pri_pool_usage_ -= lru_low_pri_->total_charge;
    low_pri_pool_usage_ += lru_low_pri_->total_charge;
  }

  while (low_pri_pool_usage_ > low_pri_pool_capacity_) {
    // Overflow last entry in low-pri pool to bottom-pri pool.
    lru_bottom_pri_ = lru_bottom_pri_->next;
    assert(lru_bottom_pri_ != &lru_);
    lru_bottom_pri_->SetInHighPriPool(false);
    lru_bottom_pri_->SetInLowPriPool(false);
    assert(low_pri_pool_usage_ >= lru_bottom_pri_->total_charge);
    low_pri_pool_usage_ -= lru_bottom_pri_->total_charge;
  }
}

void FHLRUCacheShard::EvictFromLRU(size_t charge,
                                 autovector<FHLRUHandle*>* deleted) {
  while ((usage_ + charge) > capacity_ && lru_.next != &lru_) {
    FHLRUHandle* old = lru_.next;
    // LRU list contains only elements which can be evicted.
    assert(old->InCache() && !old->HasRefs());
    LRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    old->SetInCache(false);
    assert(usage_ >= old->total_charge);
    usage_ -= old->total_charge;
    deleted->push_back(old);
  }
}

void FHLRUCacheShard::NotifyEvicted(
    const autovector<FHLRUHandle*>& evicted_handles) {
  MemoryAllocator* alloc = table_.GetAllocator();
  for (FHLRUHandle* entry : evicted_handles) {
    if (eviction_callback_ &&
        eviction_callback_(entry->key(),
                           reinterpret_cast<Cache::Handle*>(entry))) {
      // Callback took ownership of obj; just free handle
      free(entry);
    } else {
      // Free the entries here outside of mutex for performance reasons.
      entry->Free(alloc);
    }
  }
}

bool FHLRUCacheShard::RandomSample() {
  return (double)std::rand() / (RAND_MAX) < 1.0 / GRANULARITY;
}

void FHLRUCacheShard::SetCapacity(size_t capacity) {
  autovector<FHLRUHandle*> last_reference_list;
  {
    DMutexLock l(mutex_);
    capacity_ = capacity;
    high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
    low_pri_pool_capacity_ = capacity_ * low_pri_pool_ratio_;
    EvictFromLRU(0, &last_reference_list);
  }

  NotifyEvicted(last_reference_list);
}

void FHLRUCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  DMutexLock l(mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

Status FHLRUCacheShard::InsertItem(FHLRUHandle* e, FHLRUHandle** handle) {
  Status s = Status::OK();
  autovector<FHLRUHandle*> last_reference_list;

  {
    DMutexLock l(mutex_);

    // Free the space following strict LRU policy until enough space
    // is freed or the lru list is empty.
    EvictFromLRU(e->total_charge, &last_reference_list);

    if ((usage_ + e->total_charge) > capacity_ &&
        (strict_capacity_limit_ || handle == nullptr)) {
      e->SetInCache(false);
      if (handle == nullptr) {
        // Don't insert the entry but still return ok, as if the entry inserted
        // into cache and get evicted immediately.
        last_reference_list.push_back(e);
      } else {
        free(e);
        e = nullptr;
        *handle = nullptr;
        s = Status::MemoryLimit("Insert failed due to LRU cache being full.");
      }
    } else {
      // Insert into the cache. Note that the cache might get larger than its
      // capacity if not enough space was freed up.
      FHLRUHandle* old = table_.Insert(e);
      usage_ += e->total_charge;
      if (old != nullptr) {
        s = Status::OkOverwritten();
        assert(old->InCache());
        old->SetInCache(false);
        if (!old->HasRefs()) {
          // old is on LRU because it's in cache and its reference count is 0.
          LRU_Remove(old);
          assert(usage_ >= old->total_charge);
          usage_ -= old->total_charge;
          last_reference_list.push_back(old);
        }
      }
      if (handle == nullptr) {
        LRU_Insert(e);
      } else {
        // If caller already holds a ref, no need to take one here.
        if (!e->HasRefs()) {
          e->Ref();
        }
        *handle = e;
      }
    }
  }

  NotifyEvicted(last_reference_list);

  return s;
}

FHLRUHandle* FHLRUCacheShard::Lookup(const Slice& key, uint32_t hash,
                                 const Cache::CacheItemHelper* /*helper*/,
                                 Cache::CreateContext* /*create_context*/,
                                 Cache::Priority /*priority*/,
                                 Statistics* /*stats*/) {
  if (status_iops)
    handle_req_num.fetch_add(1);
  bool sample = RandomSample();
  bool status_change = false;
  if (FH_ready) {
    // Fast path in FH Cache, no LRU_Remove hence no management overhead
  FH_Lookup:
    FHLRUHandle* e = FH_table_.FH_Lookup(key, hash);
    if (e != nullptr && !e->IsTomb()) {
      e->Ref();
      if (sample)
        _fh_lookup_succ++;
      return e;
    } else {
      status_change = true;
    }
  }
  DMutexLock l(mutex_);
  // In no concurrent situation, execution reachs here either have
  // FH_ready == true && status_change == true or FH_ready == false
  // So FH_ready == true && status_change = false means FH_ready has
  // changed after last if() statement, we goto FH_Lookup directly
  // and set status_change = true
  if (FH_ready && (false == status_change) ) {
    status_change = true;
    goto FH_Lookup;
  }
  FHLRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    assert(e->InCache());
    if (!e->HasRefs()) {
      // The entry is in LRU since it's in hash and has no external
      // references.
      LRU_Remove(e);
    }
    e->Ref();
    e->SetHit();
    if (sample) 
      _lookup_succ++;
  } else {
    if (sample)
      _lookup_fail++;
  }
  return e;
}

bool FHLRUCacheShard::Ref(FHLRUHandle* e) {
  DMutexLock l(mutex_);
  // To create another reference - entry must be already externally referenced.
  assert(e->HasRefs());
  e->Ref();
  return true;
}

void FHLRUCacheShard::SetHighPriorityPoolRatio(double high_pri_pool_ratio) {
  DMutexLock l(mutex_);
  high_pri_pool_ratio_ = high_pri_pool_ratio;
  high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
  MaintainPoolSize();
}

void FHLRUCacheShard::SetLowPriorityPoolRatio(double low_pri_pool_ratio) {
  DMutexLock l(mutex_);
  low_pri_pool_ratio_ = low_pri_pool_ratio;
  low_pri_pool_capacity_ = capacity_ * low_pri_pool_ratio_;
  MaintainPoolSize();
}

bool FHLRUCacheShard::Release(FHLRUHandle* e, bool /*useful*/,
                            bool erase_if_last_ref) {
  if (e == nullptr) {
    return false;
  }
  // Simple unref handle e because e is still in LRU list 
  // (we did not remove it from LRU list when lookup)
  if (e->InFH()) {
  FH_Release:
    e->Unref();
    return false;
  }
  bool must_free;
  bool was_in_cache;
  {
    DMutexLock l(mutex_);
    if (e->InFH())
      goto FH_Release;
    must_free = e->Unref();
    was_in_cache = e->InCache();
    if (must_free && was_in_cache) {
      // The item is still in cache, and nobody else holds a reference to it.
      if (usage_ > capacity_ || erase_if_last_ref) {
        // The LRU list must be empty since the cache is full.
        assert(lru_.next == &lru_ || erase_if_last_ref);
        // Take this opportunity and remove the item.
        table_.Remove(e->key(), e->hash);
        e->SetInCache(false);
      } else {
        // Put the item back on the LRU list, and don't free it.
        LRU_Insert(e);
        must_free = false;
      }
    }
    // If about to be freed, then decrement the cache usage.
    if (must_free) {
      assert(usage_ >= e->total_charge);
      usage_ -= e->total_charge;
    }
  }

  // Free the entry here outside of mutex for performance reasons.
  if (must_free) {
    // Only call eviction callback if we're sure no one requested erasure
    // FIXME: disabled because of test churn
    if (false && was_in_cache && !erase_if_last_ref && eviction_callback_ &&
        eviction_callback_(e->key(), reinterpret_cast<Cache::Handle*>(e))) {
      // Callback took ownership of obj; just free handle
      free(e);
    } else {
      e->Free(table_.GetAllocator());
    }
  }
  return must_free;
}

FHLRUHandle* FHLRUCacheShard::CreateHandle(const Slice& key, uint32_t hash,
                                       Cache::ObjectPtr value,
                                       const Cache::CacheItemHelper* helper,
                                       size_t charge) {
  assert(helper);
  // value == nullptr is reserved for indicating failure in SecondaryCache
  assert(!(helper->IsSecondaryCacheCompatible() && value == nullptr));

  // Allocate the memory here outside of the mutex.
  // If the cache is full, we'll have to release it.
  // It shouldn't happen very often though.
  FHLRUHandle* e =
      static_cast<FHLRUHandle*>(malloc(sizeof(FHLRUHandle) - 1 + key.size()));

  e->value = value;
  e->m_flags = 0;
  e->im_flags = 0;
  e->helper = helper;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 0;
  e->next = e->prev = nullptr;
  memcpy(e->key_data, key.data(), key.size());
  e->CalcTotalCharge(charge, metadata_charge_policy_);

  return e;
}

Status FHLRUCacheShard::Insert(const Slice& key, uint32_t hash,
                             Cache::ObjectPtr value,
                             const Cache::CacheItemHelper* helper,
                             size_t charge, FHLRUHandle** handle,
                             Cache::Priority priority) {
  if (status_iops)
    handle_req_num.fetch_add(1);
  FHLRUHandle* e = CreateHandle(key, hash, value, helper, charge);
  e->SetPriority(priority);
  e->SetInCache(true);
  return InsertItem(e, handle);
}

FHLRUHandle* FHLRUCacheShard::CreateStandalone(const Slice& key, uint32_t hash,
                                           Cache::ObjectPtr value,
                                           const Cache::CacheItemHelper* helper,
                                           size_t charge,
                                           bool allow_uncharged) {
  FHLRUHandle* e = CreateHandle(key, hash, value, helper, charge);
  e->SetIsStandalone(true);
  e->Ref();

  autovector<FHLRUHandle*> last_reference_list;

  {
    DMutexLock l(mutex_);

    EvictFromLRU(e->total_charge, &last_reference_list);

    if (strict_capacity_limit_ && (usage_ + e->total_charge) > capacity_) {
      if (allow_uncharged) {
        e->total_charge = 0;
      } else {
        free(e);
        e = nullptr;
      }
    } else {
      usage_ += e->total_charge;
    }
  }

  NotifyEvicted(last_reference_list);
  return e;
}

void FHLRUCacheShard::Erase(const Slice& key, uint32_t hash) {
  if (status_iops)
    handle_req_num.fetch_add(1);
  FHLRUHandle* e;
  bool last_reference = false;
  {
    DMutexLock l(mutex_);
    e = table_.Remove(key, hash);
    if (e != nullptr) {
      assert(e->InCache());
      e->SetInCache(false);
      if (!e->HasRefs()) {
        if (!e->InFH()) {
          // The entry is in LRU since it's in hash and has no external references
          LRU_Remove(e);
          assert(usage_ >= e->total_charge);
          usage_ -= e->total_charge;
          last_reference = true;
        } else {
          // The entry is in FH LRU, set it as an tomb and do not decrement the space usage
          // because it is not really erased
          e->SetIsTomb(true);
        }
      }
    }
  }

  // Free the entry here outside of mutex for performance reasons.
  // last_reference will only be true if e != nullptr.
  if (last_reference) {
    e->Free(table_.GetAllocator());
  }
}

size_t FHLRUCacheShard::GetUsage() const {
  DMutexLock l(mutex_);
  return usage_;
}

size_t FHLRUCacheShard::GetLRUelems() const {
  DMutexLock l(mutex_);
  return lru_elems_;
}

size_t FHLRUCacheShard::GetPinnedUsage() const {
  DMutexLock l(mutex_);
  assert(usage_ >= lru_usage_);
  return usage_ - lru_usage_;
}

size_t FHLRUCacheShard::GetOccupancyCount() const {
  DMutexLock l(mutex_);
  return table_.GetOccupancyCount();
}

size_t FHLRUCacheShard::GetTableAddressCount() const {
  DMutexLock l(mutex_);
  return size_t{1} << table_.GetLengthBits();
}

void FHLRUCacheShard::AppendPrintableOptions(std::string& str) const {
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    DMutexLock l(mutex_);
    snprintf(buffer, kBufferSize, "    high_pri_pool_ratio: %.3lf\n",
             high_pri_pool_ratio_);
    snprintf(buffer + strlen(buffer), kBufferSize - strlen(buffer),
             "    low_pri_pool_ratio: %.3lf\n", low_pri_pool_ratio_);
  }
  str.append(buffer);
}

void FHLRUCacheShard::FHDebug(FHLRUHandle* e, int flag) {
  std::cout << "FH Cache Debug " << flag << " " << e->InFH() << std::endl;
}

bool FHLRUCacheShard::InsertMarker() {
  auto key = new Slice("marker", 6);
  FHLRUHandle* e = 
      static_cast<FHLRUHandle*>(malloc(sizeof(FHLRUHandle) - 1 + key->size()));
  
  e->value = nullptr;
  e->m_flags = 0;
  e->im_flags = 0;
  e->helper = nullptr;
  e->key_length = key->size();
  e->hash = 0;
  e->refs = 0;
  e->next = e->prev = nullptr;
  memcpy(e->key_data, key->data(), key->size());
  e->SetPriority(Cache::Priority::LOW);
  e->SetInCache(true);
  autovector<FHLRUHandle*> last_reference_list;

  {
    DMutexLock l(mutex_);

    // Marker only contains metadata, do not have real value
    e->CalcTotalCharge(0, metadata_charge_policy_);

    EvictFromLRU(e->total_charge, &last_reference_list);
    
    FHLRUHandle* old = table_.Insert(e);
    assert(old == nullptr);
    usage_ += e->total_charge;
    LRU_Insert(e);
  }

  NotifyEvicted(last_reference_list);

  return true;
}

bool FHLRUCacheShard::FindMarker() {
  auto key = new Slice("marker", 6);
  DMutexLock l(mutex_);
  FHLRUHandle* e = table_.Lookup(*key, 0);
  return e != nullptr;
}

bool FHLRUCacheShard::ConstructFromRatio(double ratio) {
  size_t lru_size = GetLRUelems();
  size_t lru_count = lru_size * ratio;
  DMutexLock l(mutex_);
  FHLRUHandle* old = lru_.prev;
  for(size_t i = 0; i < lru_count; i++) {
    assert(old != &lru_);
    FHLRUHandle* tmp = FH_table_.FH_Insert(old);
    assert(tmp == nullptr);
    old->SetInFH(true);
    old = old->prev;
  }
  fh_lru_.prev = lru_.prev;
  lru_.prev->next = &fh_lru_;
  fh_lru_.next = old->next;
  old->next->prev = &fh_lru_;
  lru_.prev = old;
  old->next = &lru_;

  lru_low_pri_ = lru_.prev;
  lru_bottom_pri_ = lru_.prev;
  FH_ready = true;

  // printf("Construct insert count: %ld by %.2lf ratio and %ld size\n", lru_count, ratio, lru_size);
  return true;
}

bool FHLRUCacheShard::ConstructFromMarker() {
  auto key = new Slice("marker");
  size_t count = 0;
  FHLRUHandle* e;
  DMutexLock l(mutex_);
  e = table_.Lookup(*key, 0);
  if (e == nullptr)
    return false;
  fh_lru_.prev = lru_.prev;
  lru_.prev->next = &fh_lru_;
  fh_lru_.next = e->next;
  e->next->prev = &fh_lru_;
  lru_.prev = e->prev;
  e->prev->next = &lru_;
  FHLRUHandle* old = fh_lru_.next;
  while (old != &fh_lru_) {
    FHLRUHandle *tmp = FH_table_.FH_Insert(old);
    assert(tmp == nullptr);
    old->SetInFH(true);
    old = old->next;
    count++;
  }
  table_.Remove(e->key(), e->hash);
  assert(usage_ >= e->total_charge);
  usage_ -= e->total_charge;
  lru_low_pri_ = lru_.prev;
  lru_bottom_pri_ = lru_.prev;
  FH_ready = true;
  e->Free(table_.GetAllocator());
  // printf("Construct insert count: %ld\n", count);
  return true;
}

void FHLRUCacheShard::Deconstruct() {
  autovector<FHLRUHandle*> last_reference_list;
  DMutexLock l(mutex_);
  FH_ready = false;
  FHLRUHandle* old = fh_lru_.next;
  while(old != &fh_lru_) {
    old->SetInFH(false);
    if(old->HasRefs()) {
      // std::cout << "Deconstruct debug 1, old is tomb: " << old->IsTomb() << std::endl;
      assert(!old->IsTomb());
      FHLRUHandle* e = old->next;
      old->next->prev = old->prev;
      old->prev->next = old->next;
      old->prev = old->next = nullptr;
      old = e;
      // std::cout << "Deconstruct debug 1, after operation" << std::endl;
    } else if(old->IsTomb()) {
      // std::cout << "Deconstruct debug 2" << std::endl;
      printf("find tomb!\n");
      FHLRUHandle* e = old->next;
      old->next->prev = old->prev;
      old->prev->next = old->next;
      old->prev = old->next = nullptr; // can be omitted
      last_reference_list.push_back(old);
      old = e;
    }
    else{
      // std::cout << "Deconstruct debug 3" << std::endl;
      old = old->next;
    }
  }
  auto node = lru_.prev;
  fh_lru_.prev->next = &lru_;
  fh_lru_.next->prev = node;
  lru_.prev = fh_lru_.prev;
  node->next = fh_lru_.next;

  fh_lru_.prev = &fh_lru_;
  fh_lru_.next = &fh_lru_;
  lru_low_pri_ = lru_.prev; // Only for ratio = 0
  lru_bottom_pri_ = lru_.prev;
  // std::cout << "Deconstruct debug 1, after while loop" << std::endl;
  for (auto entry : last_reference_list) {
    entry->Free(table_.GetAllocator());
  }
  FH_table_.Delete_Init();
}

inline double FHLRUCacheShard::_get_reset_miss_ratio() {
  double tmp;
  // std::cout << "lookup fail: " << _lookup_fail << " lookup succ: " << _lookup_succ << std::endl;
  if (_lookup_fail + _lookup_succ == 0)
    tmp = 1.0;
  else
    tmp = _lookup_fail * 1.0 / (_lookup_fail + _lookup_succ);
  _lookup_fail = 0;
  _lookup_succ = 0;
  return tmp;
}

inline double FHLRUCacheShard::_print_FH() {
  auto total = _lookup_fail + _lookup_succ + _fh_lookup_succ;
  // double global;
  double tmp;
  if (total == 0) {
    tmp = 1.0;
    // global = 1.0;
  } else {
    tmp = (total - _fh_lookup_succ) * 1.0 / total;
    // global = _lookup_fail * 1.0 / total;
  }
  // printf("FH lookup hit: %ld, FH lookup miss, global hit: %ld, lookup miss: %ld\n",
  //     _fh_lookup_succ, _lookup_succ, _lookup_fail);
  // printf("FH miss rate: %.3lf\n", tmp);
  // printf("Global miss rate: %.3lf\n", global);
  fflush(stdout);
  return tmp;
}

inline double FHLRUCacheShard::_print_iops_FH() {
  auto ret = handle_req_num.load();
  return ret;
}

inline void FHLRUCacheShard::_reset_FH() {
  _fh_lookup_succ = 0;
  _lookup_fail = 0;
  _lookup_succ = 0;
}

FHLRUCache::FHLRUCache(const FHLRUCacheOptions& opts) : ShardedCache(opts) {
  size_t per_shard = GetPerShardCapacity();
  MemoryAllocator* alloc = memory_allocator();
  InitShards([&](FHLRUCacheShard* cs) {
    new (cs) FHLRUCacheShard(per_shard, opts.strict_capacity_limit,
                           opts.high_pri_pool_ratio, opts.low_pri_pool_ratio,
                           opts.use_adaptive_mutex, opts.metadata_charge_policy,
                           /* max_upper_hash_bits */ 32 - opts.num_shard_bits,
                           alloc, &eviction_callback_);
  });
  std::thread new_thread(&FHLRUCache::FH_Scheduler, this);
  thd_vec.push_back(std::move(new_thread));
}

FHLRUCache::~FHLRUCache() {
  for (auto &t: thd_vec) {
    t.join();
  }
}

Learning_Input_Node FHLRUCacheShard::learning_machine(Learning_Input_Node last_sleep_node, double last_iops){
  if(last_sleep_node.ratio_ < 0) {
    //ratio_container.push(Learning_Input_Node(1, 0.5));
    ratio_container.push(Learning_Input_Node(0, 0.2));
    // ratio_container.push(Learning_Input_Node(0.2, 0.2));
    // ratio_container.push(Learning_Input_Node(0.4, 0.2));
    // ratio_container.push(Learning_Input_Node(0.6, 0.2));
    // ratio_container.push(Learning_Input_Node(0.8, 0.2));
    ratio_container.push(Learning_Input_Node(0.9, 0.9));
    auto temp = ratio_container.front();
    ratio_container.pop();
    return temp;
  }

  // printf("iops from %.4f to %.4f, sleep ratio from %.2f to %.2f\n\n",
  //     best_miss, last_iops, last_sleep, last_sleep_node.ratio_);
  
  if(! ratio_container.empty()){
    if(abs(last_sleep_node.ratio_) > EPSILON){
      learning_container.push(Learning_Result_Node(last_sleep_node.ratio_, \
        last_iops, last_sleep_node.granularity_, false));
    } else {
      learning_container.push(Learning_Result_Node(last_sleep_node.ratio_, \
        last_iops, last_sleep_node.granularity_, true));
    }
    auto temp = ratio_container.front();
    ratio_container.pop();
    best_miss = learning_container.top().metrics_;
    last_sleep = learning_container.top().ratio_;
    return temp;
  }
  else {
    learning_container.push(Learning_Result_Node(last_sleep_node.ratio_, \
      last_iops, last_sleep_node.granularity_, false));
    Learning_Result_Node todo_node = learning_container.top();
    if(todo_node.status_ == false) {
      learning_container.pop();
      learning_container.push(Learning_Result_Node(todo_node.ratio_, \
        todo_node.metrics_, todo_node.granularity_, true));
      best_miss = learning_container.top().metrics_;
      last_sleep = learning_container.top().ratio_;
      return Learning_Input_Node(todo_node.ratio_ - todo_node.granularity_/2, todo_node.granularity_/2);
    } else {
      // printf("end learning\n");
      learning_container = std::priority_queue<Learning_Result_Node, std::vector<Learning_Result_Node>, FH_cmp>();
      return Learning_Input_Node(todo_node.ratio_, 0); // end
    }
  }
}

void FHLRUCache::FH_Scheduler() {
  // double tp;
  // using namespace std::chrono;
  // typedef system_clock sys_clk_t;
  // typedef system_clock::time_point tp;
  // typedef duration<double, std::ratio<1, 100000>> micro_sec_t;
  // tp start_query;
  int count = 0;
  // printf("Large Granularity: %d\n", LARGE_GRANULARITY);
  // printf("QUERY_INTERVAL: %.2lf s\n", QUERY_INTERVAL_US/1000/1000);
  // printf("WAIT_DYNAMIC_SLEEP_INTERVAL_US: %.2lf s\n", WAIT_DYNAMIC_SLEEP_INTERVAL_US/1000/1000);
  WAIT_STABLE:
  // double last_miss_ratio = 1.1;
  double miss_ratio = 0;
  size_t last_usage = 0, usage;
  sleep(10);
  while (FH_status) {
    miss_ratio = GetShard(0)._get_reset_miss_ratio();
    usage = GetShard(0).GetUsage();
    // printf("Now shard usage is %ld, capacity is: %ld\n", usage, GetShard(0).capacity_);
    if (last_usage < usage) {
      last_usage = usage;
      // printf("(shard %d) miss ratio = %.5lf -> %.5lf with usage_: %ld\n",
      //           0, last_miss_ratio, miss_ratio, usage);
      // last_miss_ratio = miss_ratio;
      usleep(WAIT_STABLE_SLEEP_INTERVAL_US);
    } else {
      // printf("(shard %d) miss ratio = %.5lf -> %.5lf\n",
      //           0, last_miss_ratio, miss_ratio);
      break;
    }
  }
  // printf("cache is stable\n");
  fflush(stdout);
  if(miss_ratio > 0.55){
    // printf("miss ratio > 0.55, miss ratio: %lf\n", miss_ratio);
    goto WAIT_STABLE;
  }
  // auto start_learning = sys_clk_t::now();
  // printf("start tuning\n");
  double sleep_ratio = 0;
  Learning_Input_Node sleep_node;
  Learning_Input_Node last_sleep_node(-1, 0);
  double median_miss_ratio = 0.0;
  // double baseline_lat;
  double miss_ratio_array[1];
  GetShard(0).best_sleep = 0;
  GetShard(0).best_miss = 0;
  while (FH_status) {
    sleep_node = GetShard(0).learning_machine(last_sleep_node, median_miss_ratio);
    sleep_ratio = sleep_node.ratio_;
    if(abs(sleep_node.granularity_) < GetShard(0).EPSILON) {
      GetShard(0).best_sleep = sleep_ratio;
      // printf("best sleep ratio: %.2lf, best miss: %.1lf\n", GetShard(0).best_sleep, sleep_ratio);
      break;
    } else if (abs(sleep_ratio) < GetShard(0).EPSILON) {
      GetShard(0)._reset_FH();
      status_iops = true;
      GetShard(0).handle_req_num = 0;
      usleep(QUERY_INTERVAL_US);
      median_miss_ratio = GetShard(0)._print_iops_FH();
      status_iops = false;
      last_sleep_node = sleep_node;
      continue;
    }
    for (int i = 0; i < 1 && FH_status; i++) {
      if (! GetShard(0).ConstructFromRatio(sleep_ratio)) {
        // printf("construct fail\n");
        miss_ratio_array[i] = 0;
        continue;
      } else {
        // Nothing todo ?
      }
      usleep(QUERY_INTERVAL_US);
      GetShard(0)._reset_FH();
      status_iops = true;
      GetShard(0).handle_req_num = 0;
      usleep(QUERY_INTERVAL_US);
      GetShard(0)._print_FH();
      miss_ratio = GetShard(0)._print_iops_FH();
      // GetShard(0)._print_FH();
      status_iops = false;
      miss_ratio_array[i] = miss_ratio;
      GetShard(0).Deconstruct();
    }
    median_miss_ratio = miss_ratio_array[0];
    last_sleep_node = sleep_node;
  }
  // printf("searching phase ");
  // auto learning_time = (time_point_cast<micro_sec_t>(sys_clk_t::now()) - time_point_cast<micro_sec_t>(start_learning)).count() / 1000 / 1000;
  // printf("%.2lf s learning time\n", learning_time);

  if (FH_status && abs(GetShard(0).best_sleep) < GetShard(0).EPSILON) {
    goto WAIT_STABLE;
  }

  CONSTRUCT:
  // auto start_construct = sys_clk_t::now();
  usleep(QUERY_INTERVAL_US);

  for (uint32_t i = 0; i < GetNumShards(); i++) {
    construct_container.push(i);
  }
  int pass_count = 0;
  while (FH_status && !construct_container.empty()) {
    size_t pass_len = construct_container.size();
    for(size_t i = 0; i < pass_len; i++) {
      int shard_id = construct_container.front();
      if (!GetShard(shard_id).ConstructFromRatio(sleep_ratio)) {
        // printf("Construct fail %d!\n", shard_id);
        i--;
        continue;
      }
      construct_container.pop();
      construct_container.push(shard_id);
    }
    for (size_t i = 0; i < pass_len; i++) {
      int shard_id = construct_container.front();
      GetShard(shard_id)._reset_FH();
      construct_container.pop();
      construct_container.push(shard_id);
    }
    usleep(QUERY_INTERVAL_US);
    for (size_t i = 0; i < pass_len; i++) {
      int shard_id = construct_container.front();
      // printf("%ld:\n", i);
      GetShard(shard_id)._print_FH();
      construct_container.pop();
    }
    // printf("pass %d\n", pass_count++);
    if (pass_count > 4) {
      break;
    }
  }
  std::queue<int> fail_list;
  if (!construct_container.empty()) {
    auto size = construct_container.size();
    for (size_t i = 0; i < size; i++) {
      auto id = construct_container.front();
      construct_container.pop();
      construct_container.push(id);
      fail_list.push(id);
    }
  }

  // printf("Construct phase: ");

  // auto construct_time = (time_point_cast<micro_sec_t>(sys_clk_t::now()) - time_point_cast<micro_sec_t>(start_construct)).count() / 1000 / 1000;
  // printf("%.2lf s construct time\n", construct_time);

  if (fail_list.size() == GetNumShards())
    goto END;

  // start_query = sys_clk_t::now();
  // miss_ratio_count = GetShard(0).COUNT_THRESHOLD;
  count = 0;
  while (FH_status) {
    count++;
    usleep(WAIT_DYNAMIC_SLEEP_INTERVAL_US);
    // printf("check %d ", count);
    for(uint32_t i = 0; i < GetNumShards(); i++) {
      //miss_ratio = shards_[i]._print_reset_FH();
      if(!construct_container.empty() && i == (uint32_t)construct_container.front()){
        construct_container.pop();
        construct_container.push(i);
        continue;
      }
      // printf("%u:\n", i);
      GetShard(i)._print_FH();
    }
  }
  // DECONSTRUCT:
  for(uint32_t i = 0; i < GetNumShards(); i++) {
    if(!fail_list.empty() && i == (uint32_t)fail_list.front()){
        fail_list.pop();
        construct_container.pop();
        // printf("skip %d\n", i);
        GetShard(i)._print_FH();
        continue;
    }
    // printf("%d\n", i);
    GetShard(i)._print_FH();
    GetShard(i).Deconstruct();
  }
  END:
  // auto query_time = (time_point_cast<micro_sec_t>(sys_clk_t::now()) - time_point_cast<micro_sec_t>(start_query)).count() / 1000 / 1000;
  // printf("%.2lf s query\n", query_time);
  if(FH_status){
    // printf("query %.2lf s v.s. construct %.2lf s\n", query_time, construct_time);
    if(count > 5) {
      sleep(10);
      // printf("go back to construct\n");
      goto CONSTRUCT;
    } else {
      // printf("go back to wait stable\n");
      goto WAIT_STABLE;
    }
  }
  // printf("end monitor\n");
}

Cache::ObjectPtr FHLRUCache::Value(Handle* handle) {
  auto h = reinterpret_cast<const FHLRUHandle*>(handle);
  return h->value;
}

size_t FHLRUCache::GetCharge(Handle* handle) const {
  return reinterpret_cast<const FHLRUHandle*>(handle)->GetCharge(
      GetShard(0).metadata_charge_policy_);
}

const Cache::CacheItemHelper* FHLRUCache::GetCacheItemHelper(
    Handle* handle) const {
  auto h = reinterpret_cast<const FHLRUHandle*>(handle);
  return h->helper;
}

size_t FHLRUCache::TEST_GetLRUSize() {
  return SumOverShards([](FHLRUCacheShard& cs) { return cs.TEST_GetLRUSize(); });
}

double FHLRUCache::GetHighPriPoolRatio() {
  return GetShard(0).GetHighPriPoolRatio();
}

void FHLRUCache::FH_Set_Status(bool status) {
  FH_status = status;
}

bool FHLRUCache::FH_Get_Status() {
  return FH_status;
}

}  // namespace lru_cache

std::shared_ptr<Cache> FHLRUCacheOptions::MakeSharedCache() const {
  if (num_shard_bits >= 20) {
    return nullptr;  // The cache cannot be sharded into too many fine pieces.
  }
  if (high_pri_pool_ratio < 0.0 || high_pri_pool_ratio > 1.0) {
    // Invalid high_pri_pool_ratio
    return nullptr;
  }
  if (low_pri_pool_ratio < 0.0 || low_pri_pool_ratio > 1.0) {
    // Invalid low_pri_pool_ratio
    return nullptr;
  }
  if (low_pri_pool_ratio + high_pri_pool_ratio > 1.0) {
    // Invalid high_pri_pool_ratio and low_pri_pool_ratio combination
    return nullptr;
  }
  // For sanitized options
  FHLRUCacheOptions opts = *this;
  if (opts.num_shard_bits < 0) {
    opts.num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  std::shared_ptr<Cache> cache = std::make_shared<FHLRUCache>(opts);
  if (secondary_cache) {
    cache = std::make_shared<CacheWithSecondaryAdapter>(cache, secondary_cache);
  }
  return cache;
}

std::shared_ptr<GeneralCache> FHLRUCacheOptions::MakeSharedGeneralCache() const {
  if (secondary_cache) {
    // Not allowed for a GeneralCache
    return nullptr;
  }
  // Works while GeneralCache is an alias for Cache
  return MakeSharedCache();
}
}  // namespace ROCKSDB_NAMESPACE
