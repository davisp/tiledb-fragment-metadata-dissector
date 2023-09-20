#pragma once

#include <stddef.h>

#include <vector>

struct Reader {
  Reader(const char* filename);
  Reader(const std::vector<uint8_t> data);

  size_t size();
  void read(void* buf, size_t nbytes, size_t offset);

  void show_read_report();

  std::vector<uint8_t> data_;
  std::vector<uint8_t> read_map_;
};
