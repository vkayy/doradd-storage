#include "ycsb/constants.hpp"
#include "ycsb/db.hpp"
#include "pipeline.hpp"
#include "txcounter.hpp"
#ifdef STORAGE_TIER
#include "ycsb/buffer_pool.hpp"
#endif
#ifdef SIM_STORAGE
#include "ycsb/sim_storage.hpp"
#endif
#ifdef ASYNC_YIELD
#include "ycsb/uring.hpp"
#include <sched/behaviour.h>
#include <cassert>
#endif
#ifdef SPIN_YIELD
#include <chrono>
#include <immintrin.h>
#endif

#include <thread>
#ifdef STORAGE_TIER
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#define GET_COWN(_INDEX) \
  auto&& row##_INDEX = get_cown_ptr_from_addr<YCSBRow>( \
    reinterpret_cast<void*>(txm->cown_ptrs[_INDEX]));
#define GET_ROW(_INDEX) \
  auto&& row##_INDEX = index->get_row(txm->indices[_INDEX]);

#ifdef STORAGE_TIER
#  define MARK_DIRTY(_INDEX) acq_row##_INDEX->is_dirty = 1
#else
#  define MARK_DIRTY(_INDEX) ((void)0)
#endif

#define TXN(_INDEX) \
  { \
    if (write_set_l & 0x1) \
    { \
      memset(acq_row##_INDEX->payload, sum, WRITE_SIZE); \
      MARK_DIRTY(_INDEX); \
    } \
    else \
    { \
      for (int j = 0; j < ROW_SIZE; j++) \
        sum += acq_row##_INDEX->payload[j]; \
    } \
    write_set_l >>= 1; \
  }
#ifdef LOG_LATENCY
#  define M_LOG_LATENCY() \
    { \
      TxCounter::instance().log_latency(init_time); \
      TxCounter::instance().incr(); \
    }
#else
#  define M_LOG_LATENCY() \
    { \
      TxCounter::instance().incr(); \
    }
#endif

#ifdef ASYNC_YIELD
// Set rerun flag and return: runtime reschedules same work, keeps cowns held
#  define YIELD() \
    do \
    { \
      verona::rt::Behaviour::behaviour_rerun() = true; \
      return; \
    } while (0)
#endif

#ifdef STORAGE_TIER
static constexpr size_t COWN_STRIDE = 64;
static_assert(sizeof(ActualCown<YCSBRow>) <= COWN_STRIDE);
static const char* BACKING_FILE_PATH = "/tmp/ycsb_doradd_payload.bin";
static constexpr size_t BACKING_FILE_BUDGET_PCT = 20;
static constexpr size_t WARMUP_PASSES = 4;
#else
struct YCSBRow
{
  char payload[ROW_SIZE];
};
static constexpr size_t COWN_STRIDE = 1024;
#endif

struct __attribute__((packed)) YCSBTransactionMarshalled
{
  uint32_t indices[ROWS_PER_TX];
  uint16_t write_set;
  uint64_t cown_ptrs[ROWS_PER_TX];
  uint8_t pad[6];
};
static_assert(sizeof(YCSBTransactionMarshalled) == 128);

#ifdef STORAGE_TIER
template<typename... Acqs>
inline void buffer_pool_acquire_all(BufferPool* bp, Acqs&... acqs)
{
  (bp->acquire(&*acqs), ...);
}
template<typename... Acqs>
inline void buffer_pool_release_all(BufferPool* bp, Acqs&... acqs)
{
#ifdef LRU_EVICT
  bp->release_batch(&*acqs...);
#else
  (bp->release(&*acqs), ...);
#endif
}
#endif

#ifdef ASYNC_YIELD
// Counts behaviour invocations to observe yield frequency
std::atomic<uint64_t> pass_count{0};
#endif

#ifdef SPIN_YIELD
#  ifndef SPIN_BUDGET_NS
#    define SPIN_BUDGET_NS 25000 // Large enough to cover I/O and ring overhead
#  endif
// Spin-poll instrumentation, relaxed like pass_count
std::atomic<uint64_t> n_spin_loops{0};
std::atomic<uint64_t> n_spin_timeouts{0};
std::atomic<uint64_t> n_denials{0};
#endif

struct YCSBTransaction
{
public:
  static Index<YCSBRow>* index;
#ifdef STORAGE_TIER
  static BufferPool* buffer_pool;
#endif

  static int prepare_cowns(char* input)
  {
    auto txm = reinterpret_cast<YCSBTransactionMarshalled*>(input);

    for (int i = 0; i < ROWS_PER_TX; i++)
    {
      auto&& cown = index->get_row(txm->indices[i]);
      txm->cown_ptrs[i] = cown.get_base_addr();
    }

    return sizeof(YCSBTransactionMarshalled);
  }

  static int prefetch_cowns(const char* input)
  {
    auto txm = reinterpret_cast<const YCSBTransactionMarshalled*>(input);

    for (int i = 0; i < ROWS_PER_TX; i++)
      __builtin_prefetch(
        reinterpret_cast<const void*>(txm->cown_ptrs[i]), 1, 3);

    return sizeof(YCSBTransactionMarshalled);
  }

#ifdef RPC_LATENCY
  static int parse_and_process(const char* input, ts_type init_time)
#else
  static int parse_and_process(const char* input)
#endif // RPC_LATENCY
  {
    const YCSBTransactionMarshalled* txm =
      reinterpret_cast<const YCSBTransactionMarshalled*>(input);

    auto ws_cap = txm->write_set;

#if defined(INDEXER) || defined(TEST_TWO)
    GET_COWN(0);
    GET_COWN(1);
    GET_COWN(2);
    GET_COWN(3);
    GET_COWN(4);
    GET_COWN(5);
    GET_COWN(6);
    GET_COWN(7);
    GET_COWN(8);
    GET_COWN(9);
#else
    GET_ROW(0);
    GET_ROW(1);
    GET_ROW(2);
    GET_ROW(3);
    GET_ROW(4);
    GET_ROW(5);
    GET_ROW(6);
    GET_ROW(7);
    GET_ROW(8);
    GET_ROW(9);
#endif

    using AcqType = acquired_cown<YCSBRow>;
#if defined(ASYNC_YIELD) && defined(RPC_LATENCY)
    when(row0, row1, row2, row3, row4, row5, row6, row7, row8, row9)
      << [ws_cap, init_time, st = TxState{}]
#elif defined(ASYNC_YIELD)
    // Note that a patch in when.h (line 128) to support stateful lambdas is required for this
    when(row0, row1, row2, row3, row4, row5, row6, row7, row8, row9)
      << [ws_cap, st = TxState{}]
#elif defined(RPC_LATENCY)
    when(row0, row1, row2, row3, row4, row5, row6, row7, row8, row9)
      << [ws_cap, init_time]
#else
    when(row0, row1, row2, row3, row4, row5, row6, row7, row8, row9) << [ws_cap]
#endif
      (AcqType acq_row0,
       AcqType acq_row1,
       AcqType acq_row2,
       AcqType acq_row3,
       AcqType acq_row4,
       AcqType acq_row5,
       AcqType acq_row6,
       AcqType acq_row7,
       AcqType acq_row8,
       AcqType acq_row9)
#ifdef ASYNC_YIELD
      mutable
#endif
      {
#ifdef ASYNC_YIELD
        YCSBRow* rows[ROWS_PER_TX] = {
          &*acq_row0, &*acq_row1, &*acq_row2, &*acq_row3, &*acq_row4,
          &*acq_row5, &*acq_row6, &*acq_row7, &*acq_row8, &*acq_row9};
        auto* bp = YCSBTransaction::buffer_pool;

        pass_count.fetch_add(1, std::memory_order_relaxed); // One per invoke

#if !defined(SIM_STORAGE) || defined(SIM_RING)
        // Drain completions before checking misses to ensure accurate state
        uring->poll();
#endif

        if (st.phase == TxState::INIT)
        {
          st.n_miss = 0; // INIT may be entered multiple times
          for (int i = 0; i < ROWS_PER_TX; i++)
            if (rows[i]->payload == nullptr)
              st.miss_i[st.n_miss++] = static_cast<uint8_t>(i);
          assert(st.n_miss <= ROWS_PER_TX);

          if (st.n_miss > 0)
          {
            if (!bp->try_admit())
            {
#ifdef SPIN_YIELD
              n_denials.fetch_add(1, std::memory_order_relaxed);
#endif
              YIELD(); // Too many in-flight transactions
            }
            for (int i = 0; i < st.n_miss; i++)
              st.slot[i] = bp->reserve_slot();
#ifdef SIM_STORAGE
            // No ring overhead, completion waits on delta elapsed after submit_ts
            st.submit_ts = std::chrono::steady_clock::now();
#  ifdef SIM_RING
            // Variant to observe cost of ring overhead 
            for (int i = 0; i < st.n_miss; i++)
              uring->submit_read(bp->slot_addr(st.slot[i]), DISK_ROW_SIZE, 0, &st);
#  endif
#else
#  ifdef BATCH_SUBMIT
            Uring::ReadReq reqs[ROWS_PER_TX];
            for (int i = 0; i < st.n_miss; i++)
              reqs[i] = {
                bp->slot_addr(st.slot[i]),
                static_cast<off_t>(rows[st.miss_i[i]]->row_idx) * DISK_ROW_SIZE};
            uring->submit_reads(reqs, st.n_miss, DISK_ROW_SIZE, &st);
#  else
            for (int i = 0; i < st.n_miss; i++)
              uring->submit_read(
                bp->slot_addr(st.slot[i]),
                DISK_ROW_SIZE,
                static_cast<off_t>(rows[st.miss_i[i]]->row_idx) * DISK_ROW_SIZE,
                &st);
#  endif
#endif
            st.phase = TxState::WAIT;
            YIELD(); // Wait for reads to complete
          }
          st.phase = TxState::WAIT; // No misses, skip directly to WAIT phase
        }

        if (st.phase == TxState::WAIT)
        {
#ifdef SIM_STORAGE
#  ifdef SIM_RING
          if (st.n_done.load(std::memory_order_acquire) != st.n_miss)
            YIELD();
#  endif
          if (
            st.n_miss > 0 &&
            std::chrono::steady_clock::now() - st.submit_ts <
              std::chrono::nanoseconds(SIM_DELAY_NS))
            YIELD(); // Delta not yet elapsed
          for (int i = 0; i < st.n_miss; i++)
          {
            memcpy(
              bp->slot_addr(st.slot[i]),
              sim_storage->row(rows[st.miss_i[i]]->row_idx),
              DISK_ROW_SIZE);
            bp->install(rows[st.miss_i[i]], st.slot[i]);
            assert(rows[st.miss_i[i]]->payload != nullptr);
          }
#else
#  ifdef SPIN_YIELD
          if (
            st.n_miss > 0 &&
            st.n_done.load(std::memory_order_acquire) != st.n_miss)
          {
            // Bounded spin-poll before yield, restores backpressure, keeps batch submit
            auto spin_start = std::chrono::steady_clock::now();
            uint64_t loops = 0;
            for (;;)
            {
              uring->poll();
              if (st.n_done.load(std::memory_order_acquire) == st.n_miss)
                break;
              _mm_pause();
              if (
                (++loops & 0x3f) == 0 &&
                std::chrono::steady_clock::now() - spin_start >=
                  std::chrono::nanoseconds(SPIN_BUDGET_NS))
              {
                n_spin_loops.fetch_add(loops, std::memory_order_relaxed);
                n_spin_timeouts.fetch_add(1, std::memory_order_relaxed);
                YIELD(); // Budget exhausted, tail-safety fallback
              }
            }
            n_spin_loops.fetch_add(loops, std::memory_order_relaxed);
          }
#  else
          if (st.n_done.load(std::memory_order_acquire) != st.n_miss)
            YIELD(); // Still waiting on reads to complete
#  endif
          for (int i = 0; i < st.n_miss; i++)
          {
            bp->install(rows[st.miss_i[i]], st.slot[i]);
            assert(rows[st.miss_i[i]]->payload != nullptr);
          }
#endif
          if (st.n_miss > 0)
            bp->release_admit();
          st.phase = TxState::DONE;
        }

        assert(st.phase == TxState::DONE);
        bp->count_acquires(ROWS_PER_TX);
#elif defined(STORAGE_TIER)
        buffer_pool_acquire_all(
          YCSBTransaction::buffer_pool,
          acq_row0,
          acq_row1,
          acq_row2,
          acq_row3,
          acq_row4,
          acq_row5,
          acq_row6,
          acq_row7,
          acq_row8,
          acq_row9);
#endif
        uint8_t sum = 0;
        uint16_t write_set_l = ws_cap;
        int j;
        TXN(0);
        TXN(1);
        TXN(2);
        TXN(3);
        TXN(4);
        TXN(5);
        TXN(6);
        TXN(7);
        TXN(8);
        TXN(9);
#ifdef STORAGE_TIER
        buffer_pool_release_all(
          YCSBTransaction::buffer_pool,
          acq_row0,
          acq_row1,
          acq_row2,
          acq_row3,
          acq_row4,
          acq_row5,
          acq_row6,
          acq_row7,
          acq_row8,
          acq_row9);
#endif
        M_LOG_LATENCY();
      };
    return sizeof(YCSBTransactionMarshalled);
  }
  YCSBTransaction(const YCSBTransaction&) = delete;
  YCSBTransaction& operator=(const YCSBTransaction&) = delete;
};

Index<YCSBRow>* YCSBTransaction::index;
#ifdef STORAGE_TIER
BufferPool* YCSBTransaction::buffer_pool;
#endif
#if defined(ASYNC_YIELD) && (!defined(SIM_STORAGE) || defined(SIM_RING))
Uring* uring;
#endif
#ifdef SIM_STORAGE
SimStorage* sim_storage;
#endif

#ifdef STORAGE_TIER
static void warmup_buffer_pool(const char* log_name, size_t n_slots)
{
  int fd = open(log_name, O_RDONLY);
  if (fd == -1)
  {
    fprintf(
      stderr, "warmup: cannot open log %s: %s\n", log_name, strerror(errno));
    return;
  }
  struct stat sb;
  fstat(fd, &sb);
  void* base = mmap(
    nullptr, sb.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
  if (base == MAP_FAILED)
  {
    fprintf(stderr, "warmup: mmap failed: %s\n", strerror(errno));
    close(fd);
    return;
  }

  uint32_t count = *reinterpret_cast<const uint32_t*>(base);
  auto* txns = reinterpret_cast<const YCSBTransactionMarshalled*>(
    static_cast<const char*>(base) + sizeof(uint32_t));
  size_t n_txns = count;
  size_t cap = std::min(n_txns, WARMUP_PASSES * n_slots / ROWS_PER_TX);
  auto* bp = YCSBTransaction::buffer_pool;
  auto* index = YCSBTransaction::index;

#ifdef SIM_STORAGE
  sim_storage->warming = true; // Skip delta during warmup to avoid long waits on large delta
#endif
  for (size_t t = 0; t < cap; t++)
  {
    YCSBRow* rows[ROWS_PER_TX];
    for (uint32_t i = 0; i < ROWS_PER_TX; i++)
    {
      uint32_t key = txns[t].indices[i];
      rows[i] = index->get_row_addr(key)->get_ref_unsafe();
      bp->acquire(rows[i]);
    }
#ifdef LRU_EVICT
    bp->release_batch(
      rows[0], rows[1], rows[2], rows[3], rows[4],
      rows[5], rows[6], rows[7], rows[8], rows[9]);
#else
    for (uint32_t i = 0; i < ROWS_PER_TX; i++)
      bp->release(rows[i]);
#endif
  }

  munmap(base, sb.st_size);
  close(fd);
#ifdef SIM_STORAGE
  sim_storage->warming = false;
#endif
  bp->reset_stats();
  fprintf(stderr, "warmup complete: touched %zu txns\n", cap);
}
#endif

template<>
inline void pipeline_teardown<YCSBTransaction>()
{
#ifdef STORAGE_TIER
  auto* bp = YCSBTransaction::buffer_pool;
  auto* index = YCSBTransaction::index;
  for (uint64_t k = 0; k < DB_SIZE; k++)
    bp->writeback_dirty(index->get_row_addr(k)->get_ref_unsafe());
  bp->fsync_backing();
  bp->print_stats();
#ifdef ASYNC_YIELD
  bp->slot_check();
  printf(
    "passes: total=%llu per_tx=%.3f\n",
    static_cast<unsigned long long>(pass_count.load(std::memory_order_relaxed)),
    static_cast<double>(pass_count.load(std::memory_order_relaxed)) / RPC_LOG_SIZE);
#ifdef SPIN_YIELD
  printf(
    "spin: loops=%llu timeouts=%llu denials=%llu budget_ns=%d\n",
    static_cast<unsigned long long>(n_spin_loops.load(std::memory_order_relaxed)),
    static_cast<unsigned long long>(n_spin_timeouts.load(std::memory_order_relaxed)),
    static_cast<unsigned long long>(n_denials.load(std::memory_order_relaxed)),
    SPIN_BUDGET_NS);
#endif
  fflush(stdout);
#endif
#ifdef SIM_STORAGE
  if (getenv("SIM_DUMP"))
    sim_storage->dump("/tmp/ycsb_sim_payload.bin");
#endif
#endif
}

template<>
inline void pipeline_stats<YCSBTransaction>()
{
#ifdef STORAGE_TIER
  static BufferPool::Stats prev{};
  BufferPool::Stats cur = YCSBTransaction::buffer_pool->snapshot();
  printf(
    "window fetches=%llu evictions=%llu writebacks=%llu resident=%llu\n",
    static_cast<unsigned long long>(cur.fetches - prev.fetches),
    static_cast<unsigned long long>(cur.evictions - prev.evictions),
    static_cast<unsigned long long>(cur.writebacks - prev.writebacks),
    static_cast<unsigned long long>(cur.resident));
  prev = cur;
#endif
}

int main(int argc, char** argv)
{
  if (argc != 6 || strcmp(argv[1], "-n") != 0)
  {
    fprintf(
      stderr,
      "Usage: ./program -n core_cnt"
      " <dispatcher_input_file> -i <inter_arrival>\n");
    return -1;
  }

  uint8_t core_cnt = atoi(argv[2]);
  uint8_t max_core = std::thread::hardware_concurrency();
  assert(1 < core_cnt && core_cnt <= max_core);

  // Create rows (cowns) with huge pages and via static allocation
  YCSBTransaction::index = new Index<YCSBRow>;
  uint64_t cown_prev_addr = 0;
  uint8_t* cown_arr_addr =
    static_cast<uint8_t*>(aligned_alloc_hpage(COWN_STRIDE * DB_SIZE));
#ifdef STORAGE_TIER
  size_t n_slots = (uint64_t)DB_SIZE * BACKING_FILE_BUDGET_PCT / 100;
#ifdef SIM_STORAGE
  // Simulated tier has no backing file (has ring variant though)
  sim_storage = new SimStorage();
  sim_storage->init();
  int backing_fd = -1;
#else
  // Creates backing file for payload and initialises buffer pool
  int backing_fd =
    create_backing_file(BACKING_FILE_PATH, (uint64_t)DISK_ROW_SIZE * DB_SIZE);
#endif
  YCSBTransaction::buffer_pool = new BufferPool(n_slots, core_cnt, backing_fd);
#endif
#if defined(ASYNC_YIELD) && !defined(SIM_STORAGE)
  uring = new Uring();
  uring->init(backing_fd, BufferPool::MAX_INFLIGHT_TX * ROWS_PER_TX);
#endif
#ifdef SIM_RING
  // Simulated storage ring just reads a cached scratch file, takes on ring overhead
  uring = new Uring();
  uring->init(sim_ring_scratch_fd(), BufferPool::MAX_INFLIGHT_TX * ROWS_PER_TX);
#endif

  for (int i = 0; i < DB_SIZE; i++)
  {
#ifdef STORAGE_TIER
    cown_ptr<YCSBRow> cown_r = make_cown_custom<YCSBRow>(
      reinterpret_cast<void*>(cown_arr_addr + COWN_STRIDE * i),
      static_cast<uint32_t>(i));
#else
    cown_ptr<YCSBRow> cown_r = make_cown_custom<YCSBRow>(
      reinterpret_cast<void*>(cown_arr_addr + COWN_STRIDE * i));
#endif

    if (i > 0)
      assert((cown_r.get_base_addr() - cown_prev_addr) == COWN_STRIDE);
    cown_prev_addr = cown_r.get_base_addr();

    YCSBTransaction::index->insert_row(cown_r);
  }

#ifdef STORAGE_TIER
  warmup_buffer_pool(argv[3], n_slots);
#endif

  build_pipelines<YCSBTransaction>(core_cnt - 1, argv[3], argv[5]);
}
