#pragma once

#ifdef SIM_STORAGE

#  include "constants.hpp"

#  include <chrono>
#  include <cstdint>
#  include <cstdio>
#  include <cstdlib>
#  include <cstring>
#  include <fcntl.h>
#  include <sys/mman.h>
#  include <unistd.h>
#  if defined(__x86_64__) || defined(__i386__)
#    include <immintrin.h>
#  endif

// In-memory simulation of storage tier, with miss costing delay + memcpy (+ optional ring overhead)
struct SimStorage
{
  uint8_t* base = nullptr;
  // Suppresses fetch delay during single-threaded warmup (unnecessarily long with large delta)
  bool warming = false;

  void init()
  {
    size_t sz = static_cast<uint64_t>(DB_SIZE) * DISK_ROW_SIZE;
    base = static_cast<uint8_t*>(mmap(
      nullptr,
      sz,
      PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,
      -1,
      0));
    if (base == MAP_FAILED)
    {
      perror("sim mmap");
      abort();
    }
    // Byte-identical to create_backing_file fill so dump reproduces checksum
    memset(base, 0xAB, sz);
  }

  uint8_t* row(uint32_t idx) const
  {
    return base + static_cast<size_t>(idx) * DISK_ROW_SIZE;
  }

  // Dump array for reproducing checksum
  void dump(const char* path) const
  {
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
    {
      perror("sim dump open");
      abort();
    }
    size_t sz = static_cast<uint64_t>(DB_SIZE) * DISK_ROW_SIZE;
    size_t off = 0;
    while (off < sz)
    {
      ssize_t w = write(fd, base + off, sz - off);
      if (w <= 0)
      {
        perror("sim dump write");
        abort();
      }
      off += static_cast<size_t>(w);
    }
    fsync(fd);
    close(fd);
  }
};

extern SimStorage* sim_storage;

#  ifdef SIM_RING
// Optional ring, reading a cached page per miss (adds overhead of ring)
inline int sim_ring_scratch_fd()
{
  const char* path = "/tmp/ycsb_sim_ring_scratch.bin";
  int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) { perror("sim_ring scratch open"); abort(); }
  auto* buf = static_cast<uint8_t*>(malloc(DISK_ROW_SIZE));
  memset(buf, 0xCD, DISK_ROW_SIZE);
  if (write(fd, buf, DISK_ROW_SIZE) != static_cast<ssize_t>(DISK_ROW_SIZE))
  { perror("sim_ring scratch write"); abort(); }
  free(buf);
  fsync(fd);
  close(fd);
  int rfd = open(path, O_RDONLY); // buffered (page-cached), NOT O_DIRECT: no wall
  if (rfd < 0) { perror("sim_ring scratch reopen"); abort(); }
  return rfd;
}
#  endif

// Spins to mimic synchronous blocking
inline void sim_spin_until(std::chrono::steady_clock::time_point deadline)
{
  while (std::chrono::steady_clock::now() < deadline)
#  if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#  else
    ;
#  endif
}

#endif // SIM_STORAGE
