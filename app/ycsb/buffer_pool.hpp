#pragma once

#include "constants.hpp"
#include "hugepage.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

struct YCSBRow
{
  char* payload;    // Points into buffer pool when resident, nullptr otherwise
  uint32_t row_idx; // Index of the row in the database and backing file
  uint8_t is_dirty; // Tracks dirty state for eviction
#ifdef LRU_EVICT
  uint64_t last_use; // Logical timestamp of most recent access; touched only by holder
#endif

  explicit YCSBRow(uint32_t id)
  : payload(nullptr), row_idx(id), is_dirty(0)
#ifdef LRU_EVICT
    , last_use(0)
#endif
  {}
};

// Fixed number of in-memory slots for rows, with a global free list and per-thread local free lists
class BufferPool
{
public:
  // Number of slots a thread pulls/pushes when refilling/draining its local free list
  static constexpr size_t FREE_LIST_BATCH_SIZE = 32;
  // Number of slots a thread's local free list can hold before pushing back to global free list
  static constexpr size_t MAX_LOCAL_FREE_LIST_SIZE = 64;

  static_assert(MAX_LOCAL_FREE_LIST_SIZE >= FREE_LIST_BATCH_SIZE);

  // Headroom is safe heuristic; max number of slots used by each thread's active behaviour and in its local free list
  BufferPool(size_t n_slots, size_t core_cnt, int fd)
  : n_slots_(n_slots),
    headroom_(core_cnt * (MAX_LOCAL_FREE_LIST_SIZE + ROWS_PER_TX)),
    fd_(fd),
    pool_(static_cast<uint8_t*>(aligned_alloc_hpage(n_slots * DISK_ROW_SIZE))),
    n_resident_(0),
    n_acquires_(0),
    n_fetches_(0),
    n_evicts_(0)
    #ifdef LRU_EVICT
        , clock_(1)
    #endif
  {
    global_free_list_.reserve(n_slots);
    for (size_t i = n_slots; i > 0; --i)
      global_free_list_.push_back(i - 1);
    printf(
      "initialised buffer pool with %zu slots (headroom=%zu, batch size=%zu, max local size=%zu) = %zu MB\n",
      n_slots_,
      headroom_,
      FREE_LIST_BATCH_SIZE,
      MAX_LOCAL_FREE_LIST_SIZE,
      (n_slots_ * DISK_ROW_SIZE) >> 20);
  }

  // Acquires row, fetching it from backing file if not resident
  void acquire(YCSBRow* row)
  {
    n_acquires_.fetch_add(1, std::memory_order_relaxed);
    if (row->payload == nullptr)
      fetch(row);
  }

  // Releases row, evicting it if buffer pool exceeds threshold (MRU baseline)
  void release(YCSBRow* row)
  {
    if (n_resident_.load() + headroom_ > n_slots_)
      evict(row);
  }

#ifdef LRU_EVICT
  // Releases rows, evicting while buffer pool exceeds threshold (LRU)
  template<typename... Rows>
  void release_batch(Rows*... row_pack)
  {
    YCSBRow* rows[] = {row_pack...};
    constexpr size_t n = sizeof...(row_pack);
    uint64_t ts = clock_.fetch_add(1, std::memory_order_relaxed);
    while (n_resident_.load() + headroom_ > n_slots_)
    {
      YCSBRow* victim = nullptr;
      for (size_t i = 0; i < n; i++)
      {
        if (rows[i]->payload == nullptr)
          continue;
        if (victim == nullptr || rows[i]->last_use < victim->last_use)
          victim = rows[i];
      }
      if (victim == nullptr)
        break;
      evict(victim);
    }
    for (size_t i = 0; i < n; i++)
      if (rows[i]->payload != nullptr)
        rows[i]->last_use = ts;
  }
#endif

  // Zeroes hit-rate counters (used after warmup)
  void reset_stats()
  {
    n_acquires_.store(0, std::memory_order_relaxed);
    n_fetches_.store(0, std::memory_order_relaxed);
    n_evicts_.store(0, std::memory_order_relaxed);
  }

  // Prints cumulative hit-rate stats
  void print_stats()
  {
    uint64_t acq = n_acquires_.load(std::memory_order_relaxed);
    uint64_t fet = n_fetches_.load(std::memory_order_relaxed);
    double hit_rate = acq ? 1.0 - static_cast<double>(fet) / static_cast<double>(acq) : 0.0;
    printf(
      "buffer pool: acquires=%llu fetches=%llu evictions=%llu hit_rate=%.4f\n",
      static_cast<unsigned long long>(acq),
      static_cast<unsigned long long>(fet),
      static_cast<unsigned long long>(n_evicts_.load(std::memory_order_relaxed)),
      hit_rate);
    fflush(stdout);
  }

private:
  // Reads a row's payload from the backing file into the given slot, returning slot address
  char* read_into_slot(YCSBRow* row, size_t slot_idx)
  {
    char* slot_addr = reinterpret_cast<char*>(pool_ + slot_idx * DISK_ROW_SIZE);
    off_t payload_off = static_cast<off_t>(row->row_idx) * DISK_ROW_SIZE;
    ssize_t ret = pread(fd_, slot_addr, DISK_ROW_SIZE, payload_off);
    if (ret != static_cast<ssize_t>(DISK_ROW_SIZE))
    {
      struct stat sb;
      fstat(fd_, &sb);
      fprintf(
        stderr,
        "failed to read row %u: ret=%zd off=%lld len=%u filesz=%lld "
        "buf_mod512=%lu off_mod512=%lld errno=%s\n",
        row->row_idx, ret, (long long)payload_off, (unsigned)DISK_ROW_SIZE,
        (long long)sb.st_size,
        (unsigned long)(reinterpret_cast<uintptr_t>(slot_addr) % 512),
        (long long)(payload_off % 512), strerror(errno));
      abort();
    }
    return slot_addr;
  }

  // Finds free slot in memory, reads payload from backing file into it, and updates row's payload pointer
  void fetch(YCSBRow* row)
  {
    size_t slot_idx = obtain_slot_idx();
    row->payload = read_into_slot(row, slot_idx);
    n_resident_.fetch_add(1, std::memory_order_relaxed);
    n_fetches_.fetch_add(1, std::memory_order_relaxed);
  }

  // Writes back if dirty, marks row as non-resident, and returns slot index to free list
  void evict(YCSBRow* row)
  {
    size_t slot_idx = (reinterpret_cast<uint8_t*>(row->payload) - pool_) / DISK_ROW_SIZE;
    if (row->is_dirty)
    {
      off_t payload_off = static_cast<off_t>(row->row_idx) * DISK_ROW_SIZE;
      if (
        pwrite(fd_, row->payload, DISK_ROW_SIZE, payload_off) !=
        static_cast<ssize_t>(DISK_ROW_SIZE))
      {
        fprintf(stderr, "failed to write row %u: %s\n", row->row_idx, strerror(errno));
        abort();
      }
      row->is_dirty = 0;
    }
    row->payload = nullptr;
    n_resident_.fetch_sub(1, std::memory_order_relaxed);
    n_evicts_.fetch_add(1, std::memory_order_relaxed);
    return_slot_idx(slot_idx);
  }

  // Check local free list first, refilling from global free list if empty
  size_t obtain_slot_idx()
  {
    if (local_free_list_.empty())
      refill_local_free_list();
    size_t slot_idx = local_free_list_.back();
    local_free_list_.pop_back();
    return slot_idx;
  }

  // Returns a slot index to the local free list, draining to global free list if full
  void return_slot_idx(size_t slot_idx)
  {
    if (local_free_list_.size() == MAX_LOCAL_FREE_LIST_SIZE)
      drain_local_free_list();
    local_free_list_.push_back(slot_idx);
  }

  // Refills local free list from global free list in batches to reduce contention
  void refill_local_free_list()
  {
    // Reserve space for local free list on first use to avoid dynamic resizing
    if (local_free_list_.capacity() < MAX_LOCAL_FREE_LIST_SIZE)
      local_free_list_.reserve(MAX_LOCAL_FREE_LIST_SIZE);

    std::lock_guard<std::mutex> lock(mutex_);
    size_t n_refill = std::min(FREE_LIST_BATCH_SIZE, global_free_list_.size());
    if (n_refill == 0)
    {
      fprintf(
        stderr,
        "buffer pool: free list exhausted (n_slots=%zu, headroom=%zu)\n",
        n_slots_,
        headroom_);
      abort();
    }
    for (size_t i = 0; i < n_refill; i++)
    {
      local_free_list_.push_back(global_free_list_.back());
      global_free_list_.pop_back();
    }
  }

  // Drains local free list back to global free list in batches to reduce contention
  void drain_local_free_list()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // Only called when local free list is at max capacity which is greater than batch size
    for (size_t i = 0; i < FREE_LIST_BATCH_SIZE; i++)
    {
      global_free_list_.push_back(local_free_list_.back());
      local_free_list_.pop_back();
    }
  }

  mutable std::mutex mutex_;
  const size_t n_slots_;
  const size_t headroom_;
  const int fd_;
  uint8_t* const pool_;
  std::atomic<size_t> n_resident_;
  std::atomic<uint64_t> n_acquires_;
  std::atomic<uint64_t> n_fetches_;
  std::atomic<uint64_t> n_evicts_;
  #ifdef LRU_EVICT
    std::atomic<uint64_t> clock_;
  #endif
  std::vector<size_t> global_free_list_;
  static inline thread_local std::vector<size_t> local_free_list_;
};
