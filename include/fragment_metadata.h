#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "reader.h"
#include "tile.h"

struct GenericTileOffsets {
  GenericTileOffsets(size_t nfields);

  uint64_t rtree_ = 0;
  std::vector<uint64_t> tile_offsets_;
  std::vector<uint64_t> tile_var_offsets_;
  std::vector<uint64_t> tile_var_sizes_;
  std::vector<uint64_t> tile_validity_offsets_;
  std::vector<uint64_t> tile_min_offsets_;
  std::vector<uint64_t> tile_max_offsets_;
  std::vector<uint64_t> tile_sum_offsets_;
  std::vector<uint64_t> tile_null_count_offsets_;
  uint64_t fragment_min_max_sum_null_count_offset_;
  uint64_t processed_conditions_offsets_;
};

struct Footer {
  Footer(Reader& reader, size_t nfields);

  void load_tile_offsets(Reader& reader, uint64_t offset, std::vector<uint64_t>& dst);

  void dump();

  uint64_t fragment_metadata_file_size_;
  uint64_t footer_size_;
  uint64_t footer_offset_;
  uint32_t version_;
  std::string array_schema_;
  uint8_t fragment_type_;
  uint8_t null_non_empty_domain_;
  double non_empty_domain_[4];
  uint64_t sparse_tile_num_;
  uint64_t last_tile_cell_num_;
  uint8_t has_timestamps_;
  uint8_t has_delete_meta_;

  std::vector<uint64_t> file_sizes_;
  std::vector<uint64_t> file_var_sizes_;
  std::vector<uint64_t> file_validity_sizes_;

  GenericTileOffsets gt_offsets_;
};

struct FragmentMetadata {
  FragmentMetadata(Reader& reader, size_t nfields);

  void load_offsets(Reader& reader, uint64_t offset, std::vector<uint64_t>& dst);
  void load_values(Reader& reader, uint64_t offset, std::vector<uint8_t>& data, std::vector<uint8_t>& var_data);
  void load_sums(Reader& reader, uint64_t offset, std::vector<uint8_t>& sums);
  void load_null_counts(Reader& reader, uint64_t offset, std::vector<uint64_t>& null_counts);
  void load_fragment_min_max_sum_null_count(Reader& reader, uint64_t offset);

  void dump();

  size_t nfields_;
  Footer footer_;

  Tile rtree_tile_;
  std::vector<std::vector<uint64_t>> tile_offsets_;
  std::vector<std::vector<uint64_t>> tile_var_offsets_;
  std::vector<std::vector<uint64_t>> tile_var_sizes_;
  std::vector<std::vector<uint64_t>> tile_validity_offsets_;

  std::vector<std::vector<uint8_t>> tile_min_;
  std::vector<std::vector<uint8_t>> tile_min_var_;
  std::vector<std::vector<uint8_t>> tile_max_;
  std::vector<std::vector<uint8_t>> tile_max_var_;
  std::vector<std::vector<uint8_t>> tile_sum_;
  std::vector<std::vector<uint64_t>> tile_null_count_;

  std::vector<std::vector<uint8_t>> fragment_min_;
  std::vector<std::vector<uint8_t>> fragment_max_;
  std::vector<uint64_t> fragment_sum_;
  std::vector<uint64_t> fragment_null_count_;

  Tile processed_conditions_tile_;
};
