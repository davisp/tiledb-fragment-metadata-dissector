#pragma once

#include <stddef.h>

#include <vector>

struct Reader {
  Reader(const char* filename);

  void read(void* buf, size_t nbytes, size_t offset);
  void show_read_report();

  int fd_;
  uint64_t file_size_;
  std::vector<uint8_t> read_map_;
};
