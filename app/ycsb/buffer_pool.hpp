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

#ifdef ASYNC_YIELD
// Slots are DISK_ROW_SIZE-strided off a hugepage base; 512-alignment for O_DIRECT
static_assert(DISK_ROW_SIZE % 512 == 0,
  "ASYNC_YIELD requires DISK_ROW_SIZE 512-aligned (build with DIRECT_IO)");
#endif

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

#ifdef ASYNC_YIELD
  // Caps concurrently in-flight missing transactions (queue-depth knob)
  static constexpr size_t MAX_INFLIGHT_TX = 64;
#endif

  // Headroom is safe heuristic; max number of slots used by each thread's active behaviour and in its local free list
  BufferPool(size_t n_slots, size_t core_cnt, int fd)
  : n_slots_(n_slots),
#ifdef ASYNC_YIELD
    // Parked behaviour holds up to ROWS_PER_TX reserved slots; admission bounds them
    headroom_(core_cnt * MAX_LOCAL_FREE_LIST_SIZE + MAX_INFLIGHT_TX * ROWS_PER_TX),
#else
    headroom_(core_cnt * (MAX_LOCAL_FREE_LIST_SIZE + ROWS_PER_TX)),
#endif
    core_cnt_(core_cnt),
    fd_(fd),
    pool_(static_cast<uint8_t*>(aligned_alloc_hpage(n_slots * DISK_ROW_SIZE))),
    n_resident_(0),
    n_acquires_(0),
    n_fetches_(0),
    n_evicts_(0),
    n_writebacks_(0)
    #ifdef ASYNC_YIELD
        , inflight_(0)
    #endif
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

  struct Stats
  {
    uint64_t acquires;
    uint64_t fetches;
    uint64_t evictions;
    uint64_t writebacks;
    uint64_t resident;
  };

  // Relaxed snapshot of cumulative counters for per-window deltas
  Stats snapshot() const
  {
    return Stats{
      n_acquires_.load(std::memory_order_relaxed),
      n_fetches_.load(std::memory_order_relaxed),
      n_evicts_.load(std::memory_order_relaxed),
      n_writebacks_.load(std::memory_order_relaxed),
      n_resident_.load(std::memory_order_relaxed)};
  }

  // Write back resident dirty rows for repeatable final state (teardown)
  void writeback_dirty(YCSBRow* row)
  {
    if (row->payload == nullptr || !row->is_dirty)
      return;
    off_t payload_off = static_cast<off_t>(row->row_idx) * DISK_ROW_SIZE;
    if (
      pwrite(fd_, row->payload, DISK_ROW_SIZE, payload_off) !=
      static_cast<ssize_t>(DISK_ROW_SIZE))
    {
      fprintf(stderr, "flush: failed to write row %u: %s\n", row->row_idx, strerror(errno));
      abort();
    }
    row->is_dirty = 0;
  }

  // Sync backing file after final writebacks (teardown)
  void fsync_backing()
  {
    fsync(fd_);
  }

#ifdef ASYNC_YIELD
  // Reserve half of fetch: take a slot, NOT yet resident (read fills it async)
  size_t reserve_slot()
  {
    return obtain_slot_idx();
  }

  char* slot_addr(size_t slot_idx) const
  {
    return reinterpret_cast<char*>(pool_ + slot_idx * DISK_ROW_SIZE);
  }

  // Install half of fetch: payload now valid (read completed), count residency
  void install(YCSBRow* row, size_t slot_idx)
  {
    row->payload = slot_addr(slot_idx);
    n_resident_.fetch_add(1, std::memory_order_relaxed);
    n_fetches_.fetch_add(1, std::memory_order_relaxed);
  }

  // Admission: only allow fetch if we won't exceed max in-flight transactions (queue-depth knob)
  bool try_admit()
  {
    size_t cur = inflight_.load(std::memory_order_relaxed);
    while (cur < MAX_INFLIGHT_TX)
      if (inflight_.compare_exchange_weak(
            cur, cur + 1, std::memory_order_acq_rel, std::memory_order_relaxed))
        return true;
    return false;
  }

  void release_admit()
  {
    inflight_.fetch_sub(1, std::memory_order_acq_rel);
  }

  // Counted once on the final pass (per-pass counting corrupts hit_rate)
  void count_acquires(size_t n)
  {
    n_acquires_.fetch_add(n, std::memory_order_relaxed);
  }

  // Check that free list sizes are within expected bounds (teardown)
  void slot_check() const
  {
    size_t global = global_free_list_.size();
    size_t resident = n_resident_.load(std::memory_order_relaxed);
    size_t local_bound = core_cnt_ * MAX_LOCAL_FREE_LIST_SIZE;
    long residual = static_cast<long>(n_slots_) - static_cast<long>(global) -
      static_cast<long>(resident);
    bool pass = residual >= 0 && static_cast<size_t>(residual) <= local_bound;
    printf(
      "slot check: n_slots=%zu global_free=%zu resident=%zu residual=%ld "
      "local_bound=%zu %s\n",
      n_slots_, global, resident, residual, local_bound, pass ? "PASS" : "FAIL");
    fflush(stdout);
  }
#endif

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
      n_writebacks_.fetch_add(1, std::memory_order_relaxed);
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
  const size_t core_cnt_;
  const int fd_;
  uint8_t* const pool_;
  std::atomic<size_t> n_resident_;
  std::atomic<uint64_t> n_acquires_;
  std::atomic<uint64_t> n_fetches_;
  std::atomic<uint64_t> n_evicts_;
  std::atomic<uint64_t> n_writebacks_;
  #ifdef ASYNC_YIELD
    std::atomic<size_t> inflight_;
  #endif
  #ifdef LRU_EVICT
    std::atomic<uint64_t> clock_;
  #endif
  std::vector<size_t> global_free_list_;
  static inline thread_local std::vector<size_t> local_free_list_;
};
