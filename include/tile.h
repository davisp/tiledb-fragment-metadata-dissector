
#pragma once

#include <cstdint>
#include <vector>

#include "reader.h"

struct DiskLayout {
  DiskLayout();
  void dump();

  uint32_t unfiltered_data_size_;
  uint64_t unfiltered_data_offset_;
  uint32_t filtered_data_size_;
  uint32_t filtered_metadata_size_;
  uint8_t* filtered_metadata_;
  uint8_t* filtered_data_;
};

struct ChunkData {
  ChunkData(uint8_t* buf, size_t nbytes);

  size_t size() {
    return filtered_chunks_.size();
  }

  void dump();

  std::vector<DiskLayout> filtered_chunks_;
  uint64_t orig_size_;
};

struct Tile {
  Tile() {
  }

  Tile(uint32_t version, uint8_t datatype, uint64_t cell_size, size_t data_size)
      : version_(version)
      , datatype_(datatype)
      , cell_size_(cell_size)
      , data_(data_size) {
  }

  void dump();

  uint32_t version_;
  uint8_t datatype_;
  uint64_t cell_size_;
  std::vector<uint8_t> data_;
};

Tile read_tile(Reader& reader, uint64_t offset);
