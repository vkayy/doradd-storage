#pragma once

#ifdef ASYNC_YIELD

#  include "constants.hpp"

#  include <atomic>
#  include <cstdint>
#  include <cstdio>
#  include <cstdlib>
#  include <cstring>
#  include <mutex>
#  ifdef SIM_STORAGE
#    include <chrono>
#  endif
#  if !defined(SIM_STORAGE) || defined(SIM_RING)
#    include <liburing.h>
#    include <sys/stat.h>
#  endif

// Per-transaction async state
struct TxState
{
  enum Phase : uint8_t
  {
    INIT,
    WAIT,
    DONE
  };
  Phase phase = INIT;
  uint8_t n_miss = 0;
  std::atomic<uint8_t> n_done{0};
  uint8_t miss_i[ROWS_PER_TX];
  size_t slot[ROWS_PER_TX];
#  ifdef SIM_STORAGE
  // Submit time of miss batch, so completion waits for delta to elapse after submit_ts
  std::chrono::steady_clock::time_point submit_ts;
#  endif

  TxState() = default;

  // Moved once at schedule time (no IO in flight)
  TxState(TxState&& o) noexcept
  : phase(o.phase),
    n_miss(o.n_miss),
    n_done(o.n_done.load(std::memory_order_relaxed))
#  ifdef SIM_STORAGE
    , submit_ts(o.submit_ts)
#  endif
  {
    for (uint32_t i = 0; i < ROWS_PER_TX; i++)
    {
      miss_i[i] = o.miss_i[i];
      slot[i] = o.slot[i];
    }
  }
};

#  if !defined(SIM_STORAGE) || defined(SIM_RING)
// One global ring, one mutex guarding both submit and reap
struct Uring
{
  io_uring ring;
  std::mutex mu;
  int fd;

  void init(int backing_fd, unsigned entries)
  {
    fd = backing_fd;
    int ret = io_uring_queue_init(entries, &ring, 0);
    if (ret < 0)
    {
      fprintf(stderr, "io_uring_queue_init failed: %s\n", strerror(-ret));
      abort();
    }
  }

  // Wraps io_uring submission of a single read, associating it with the transaction state
  void submit_read(void* buf, size_t len, off_t off, void* user_data)
  {
    std::lock_guard<std::mutex> lock(mu);
    io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    if (!sqe)
    {
      fprintf(stderr, "io_uring_get_sqe: submission queue full\n");
      abort();
    }
    io_uring_prep_read(sqe, fd, buf, len, off);
    io_uring_sqe_set_data(sqe, user_data);
    int ret = io_uring_submit(&ring);
    if (ret < 0)
    {
      fprintf(stderr, "io_uring_submit failed: %s\n", strerror(-ret));
      abort();
    }
  }

  // Polls for completions, updating state of transactions with completed reads
  void poll()
  {
    if (!mu.try_lock())
      return;
    io_uring_cqe* cqe;
    while (io_uring_peek_cqe(&ring, &cqe) == 0)
    {
      if (cqe->res != static_cast<int>(DISK_ROW_SIZE))
      {
        struct stat sb;
        fstat(fd, &sb);
        fprintf(
          stderr,
          "uring cqe error: res=%d expected=%u filesz=%lld\n",
          cqe->res,
          static_cast<unsigned>(DISK_ROW_SIZE),
          static_cast<long long>(sb.st_size));
        abort();
      }
      // Release pairs with acquire load before install so fetches visible
      static_cast<TxState*>(io_uring_cqe_get_data(cqe))
        ->n_done.fetch_add(1, std::memory_order_release);
      io_uring_cqe_seen(&ring, cqe);
    }
    mu.unlock();
  }
};

extern Uring* uring;
#  endif // !SIM_STORAGE || SIM_RING

#endif // ASYNC_YIELD
