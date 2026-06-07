#pragma once

#include <stdint.h>

static constexpr uint32_t ROWS_PER_TX = 10;
static constexpr uint32_t ROW_SIZE = 900;
static constexpr uint32_t WRITE_SIZE = 100;
#ifdef DIRECT_IO
// O_DIRECT requires 512 B-aligned buffer/offset/length; payload uses first ROW_SIZE bytes
static constexpr uint32_t DISK_ROW_SIZE = (ROW_SIZE + 511) / 512 * 512;
#else
static constexpr uint32_t DISK_ROW_SIZE = ROW_SIZE;
#endif
static const uint64_t DB_SIZE = 10'000'000;
#ifdef SIM_STORAGE
// Simulated device latency (symmetric read/write delay)
static constexpr uint64_t SIM_DELAY_NS = 100000;
#endif
// const uint64_t PENDING_THRESHOLD = 100'000;
// const uint64_t SPAWN_THRESHOLD = 100'000;
