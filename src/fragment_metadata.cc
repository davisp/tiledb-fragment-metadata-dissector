
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "deserializer.h"
#include "fragment_metadata.h"
#include "reader.h"
#include "tile.h"

GenericTileOffsets::GenericTileOffsets(size_t nfields)
    : tile_offsets_(nfields)
    , tile_var_offsets_(nfields)
    , tile_var_sizes_(nfields)
    , tile_validity_offsets_(nfields)
    , tile_min_offsets_(nfields)
    , tile_max_offsets_(nfields)
    , tile_sum_offsets_(nfields)
    , tile_null_count_offsets_(nfields) {
}

Footer::Footer(Reader& reader, size_t nfields)
    : file_sizes_(nfields)
    , file_var_sizes_(nfields)
    , file_validity_sizes_(nfields)
    , gt_offsets_(nfields) {
  reader.read(&footer_size_, 8, reader.file_size_ - 8);
  footer_offset_ = reader.file_size_ - footer_size_ - 8;

  std::vector<uint8_t> footer_blob(footer_size_);
  reader.read(footer_blob.data(), footer_size_, footer_offset_);

  Deserializer dser(footer_blob.data(), footer_blob.size());
  version_ = dser.read<uint32_t>();

  uint64_t schema_name_size = dser.read<uint64_t>();
  array_schema_.resize(schema_name_size);
  dser.read(&array_schema_[0], schema_name_size);

  fragment_type_ = dser.read<uint8_t>();

  null_non_empty_domain_ = dser.read<uint8_t>();
  if (null_non_empty_domain_ == 0) {
    for (size_t i = 0; i < 4; i++) {
      non_empty_domain_[i] = dser.read<double>();
    }
  }

  sparse_tile_num_ = dser.read<uint64_t>();
  last_tile_cell_num_ = dser.read<uint64_t>();
  has_timestamps_ = dser.read<uint8_t>();
  has_delete_meta_ = dser.read<uint8_t>();

  dser.read(&file_sizes_[0], nfields * sizeof(uint64_t));
  dser.read(&file_var_sizes_[0], nfields * sizeof(uint64_t));
  dser.read(&file_validity_sizes_[0], nfields * sizeof(uint64_t));

  gt_offsets_.rtree_ = dser.read<uint64_t>();
  dser.read(&gt_offsets_.tile_offsets_[0], nfields * sizeof(uint64_t));
  dser.read(&gt_offsets_.tile_var_offsets_[0], nfields * sizeof(uint64_t));
  dser.read(&gt_offsets_.tile_var_sizes_[0], nfields * sizeof(uint64_t));
  dser.read(&gt_offsets_.tile_validity_offsets_[0], nfields * sizeof(uint64_t));
  dser.read(&gt_offsets_.tile_min_offsets_[0], nfields * sizeof(uint64_t));
  dser.read(&gt_offsets_.tile_max_offsets_[0], nfields * sizeof(uint64_t));
  dser.read(&gt_offsets_.tile_sum_offsets_[0], nfields * sizeof(uint64_t));
  dser.read(
      &gt_offsets_.tile_null_count_offsets_[0], nfields * sizeof(uint64_t));
  gt_offsets_.fragment_min_max_sum_null_count_offset_ = dser.read<uint64_t>();
  gt_offsets_.processed_conditions_offsets_ = dser.read<uint64_t>();
}

void Footer::dump() {
  fprintf(stderr, "File size: %llu\n", fragment_metadata_file_size_);
  fprintf(stderr, "Footer:\n");
  fprintf(stderr, "    Size: %llu\n", footer_size_);
  fprintf(stderr, "    Offset: %llu\n", footer_offset_);
  fprintf(stderr, "    Version: %u\n", version_);
  fprintf(stderr, "    Schema: %s\n", array_schema_.c_str());
  fprintf(stderr, "    Type: %u\n", fragment_type_);
  fprintf(stderr, "    Non-Empty Domain:\n");
  for (size_t i = 0; i < 4; i++) {
    fprintf(stderr, "        %f\n", non_empty_domain_[i]);
  }
  fprintf(stderr, "    Sparse Tile Num: %llu\n", sparse_tile_num_);
  fprintf(stderr, "    Last Tile Cell Num: %llu\n", last_tile_cell_num_);
  fprintf(stderr, "    Has Timestamps: %u\n", has_timestamps_);
  fprintf(stderr, "    Has Delete Meta: %u\n", has_delete_meta_);
  fprintf(stderr, "    File Sizes:\n");
  for (size_t i = 0; i < file_sizes_.size(); i++) {
    fprintf(stderr, "        %lu: %llu\n", i, file_sizes_[i]);
  }
  fprintf(stderr, "    File Var Sizes:\n");
  for (size_t i = 0; i < file_var_sizes_.size(); i++) {
    fprintf(stderr, "        %lu: %llu\n", i, file_var_sizes_[i]);
  }
  fprintf(stderr, "    File Validity Sizes:\n");
  for (size_t i = 0; i < file_validity_sizes_.size(); i++) {
    fprintf(stderr, "        %lu: %llu\n", i, file_validity_sizes_[i]);
  }
  fprintf(stderr, "    Genric Tile Offsets:\n");
  fprintf(stderr, "        RTree: %llu\n", gt_offsets_.rtree_);
  fprintf(stderr, "        Tile Offsets:\n");
  for (size_t i = 0; i < gt_offsets_.tile_offsets_.size(); i++) {
    fprintf(stderr, "            %lu: %llu\n", i, gt_offsets_.tile_offsets_[i]);
  }
  fprintf(stderr, "        Tile Var Offsets:\n");
  for (size_t i = 0; i < gt_offsets_.tile_var_offsets_.size(); i++) {
    fprintf(
        stderr, "            %lu: %llu\n", i, gt_offsets_.tile_var_offsets_[i]);
  }
  fprintf(stderr, "        Tile Var Sizes:\n");
  for (size_t i = 0; i < gt_offsets_.tile_var_sizes_.size(); i++) {
    fprintf(
        stderr, "            %lu: %llu\n", i, gt_offsets_.tile_var_sizes_[i]);
  }
  fprintf(stderr, "        Tile Validity Offsets:\n");
  for (size_t i = 0; i < gt_offsets_.tile_validity_offsets_.size(); i++) {
    fprintf(
        stderr,
        "            %lu: %llu\n",
        i,
        gt_offsets_.tile_validity_offsets_[i]);
  }
  fprintf(stderr, "        Tile Min Offsets:\n");
  for (size_t i = 0; i < gt_offsets_.tile_min_offsets_.size(); i++) {
    fprintf(
        stderr, "            %lu: %llu\n", i, gt_offsets_.tile_min_offsets_[i]);
  }
  fprintf(stderr, "        Tile Max Offsets:\n");
  for (size_t i = 0; i < gt_offsets_.tile_max_offsets_.size(); i++) {
    fprintf(
        stderr, "            %lu: %llu\n", i, gt_offsets_.tile_max_offsets_[i]);
  }
  fprintf(stderr, "        Tile Sum Offsets:\n");
  for (size_t i = 0; i < gt_offsets_.tile_sum_offsets_.size(); i++) {
    fprintf(
        stderr, "            %lu: %llu\n", i, gt_offsets_.tile_sum_offsets_[i]);
  }
  fprintf(stderr, "        Tile Null Count Offsets:\n");
  for (size_t i = 0; i < gt_offsets_.tile_null_count_offsets_.size(); i++) {
    fprintf(
        stderr,
        "            %lu: %llu\n",
        i,
        gt_offsets_.tile_null_count_offsets_[i]);
  }
  fprintf(
      stderr,
      "        Fragment Min/Max/Sum/Null Count Offset: %llu\n",
      gt_offsets_.fragment_min_max_sum_null_count_offset_);
  fprintf(
      stderr,
      "        Processed Conditions Offsets: %llu\n",
      gt_offsets_.processed_conditions_offsets_);
}

FragmentMetadata::FragmentMetadata(Reader& reader, size_t nfields)
    : nfields_(nfields)
    , footer_(reader, nfields)
    , tile_offsets_(nfields)
    , tile_var_offsets_(nfields)
    , tile_var_sizes_(nfields)
    , tile_validity_offsets_(nfields)
    , tile_min_(nfields)
    , tile_min_var_(nfields)
    , tile_max_(nfields)
    , tile_max_var_(nfields)
    , tile_sum_(nfields)
    , tile_null_count_(nfields)
    , fragment_min_(nfields)
    , fragment_max_(nfields)
    , fragment_sum_(nfields)
    , fragment_null_count_(nfields) {
  rtree_tile_ = read_tile(reader, footer_.gt_offsets_.rtree_);

  for (size_t i = 0; i < footer_.gt_offsets_.tile_offsets_.size(); i++) {
    load_offsets(
        reader, footer_.gt_offsets_.tile_offsets_[i], tile_offsets_[i]);
  }

  for (size_t i = 0; i < footer_.gt_offsets_.tile_var_offsets_.size(); i++) {
    load_offsets(
        reader, footer_.gt_offsets_.tile_var_offsets_[i], tile_var_offsets_[i]);
  }

  for (size_t i = 0; i < footer_.gt_offsets_.tile_var_sizes_.size(); i++) {
    load_offsets(
        reader, footer_.gt_offsets_.tile_var_sizes_[i], tile_var_sizes_[i]);
  }

  for (size_t i = 0; i < footer_.gt_offsets_.tile_validity_offsets_.size();
       i++) {
    load_offsets(
        reader,
        footer_.gt_offsets_.tile_validity_offsets_[i],
        tile_validity_offsets_[i]);
  }

  for (size_t i = 0; i < footer_.gt_offsets_.tile_min_offsets_.size(); i++) {
    load_values(
        reader,
        footer_.gt_offsets_.tile_min_offsets_[i],
        tile_min_[i],
        tile_min_var_[i]);
  }

  for (size_t i = 0; i < footer_.gt_offsets_.tile_max_offsets_.size(); i++) {
    load_values(
        reader,
        footer_.gt_offsets_.tile_max_offsets_[i],
        tile_max_[i],
        tile_max_var_[i]);
  }

  for (size_t i = 0; i < footer_.gt_offsets_.tile_sum_offsets_.size(); i++) {
    load_sums(reader, footer_.gt_offsets_.tile_sum_offsets_[i], tile_sum_[i]);
  }

  for (size_t i = 0; i < footer_.gt_offsets_.tile_null_count_offsets_.size();
       i++) {
    load_null_counts(
        reader,
        footer_.gt_offsets_.tile_null_count_offsets_[i],
        tile_null_count_[i]);
  }

  load_fragment_min_max_sum_null_count(
      reader, footer_.gt_offsets_.fragment_min_max_sum_null_count_offset_);
  processed_conditions_tile_ =
      read_tile(reader, footer_.gt_offsets_.processed_conditions_offsets_);
}

void FragmentMetadata::load_offsets(
    Reader& reader, uint64_t offset, std::vector<uint64_t>& dst) {
  Tile tile = read_tile(reader, offset);
  Deserializer dser(tile.data_.data(), tile.data_.size());

  auto num_offsets = dser.read<uint64_t>();
  if (num_offsets == 0) {
    return;
  }

  auto size = num_offsets * sizeof(uint64_t);
  dst.resize(num_offsets);
  dser.read(&dst[0], size);
}

void FragmentMetadata::load_values(
    Reader& reader,
    uint64_t offset,
    std::vector<uint8_t>& data,
    std::vector<uint8_t>& var_data) {
  Tile tile = read_tile(reader, offset);
  Deserializer dser(tile.data_.data(), tile.data_.size());

  auto data_size = dser.read<uint64_t>();
  auto var_data_size = dser.read<uint64_t>();

  data.resize(data_size);
  dser.read(&data[0], data_size);

  if (var_data_size) {
    var_data.resize(var_data_size);
    dser.read(&var_data[0], var_data_size);
  }
}

void FragmentMetadata::load_sums(
    Reader& reader, uint64_t offset, std::vector<uint8_t>& sums) {
  Tile tile = read_tile(reader, offset);
  Deserializer dser(tile.data_.data(), tile.data_.size());

  auto size = dser.read<uint64_t>();
  sums.resize(size);
  dser.read(&sums[0], size);
}

void FragmentMetadata::load_null_counts(
    Reader& reader, uint64_t offset, std::vector<uint64_t>& null_counts) {
  Tile tile = read_tile(reader, offset);
  Deserializer dser(tile.data_.data(), tile.data_.size());

  auto num_counts = dser.read<uint64_t>();
  null_counts.resize(num_counts);

  auto size = num_counts * sizeof(uint64_t);
  dser.read(&null_counts[0], size);
}

void FragmentMetadata::load_fragment_min_max_sum_null_count(
    Reader& reader, uint64_t offset) {
  Tile tile = read_tile(reader, offset);
  Deserializer dser(tile.data_.data(), tile.data_.size());

  for (unsigned int i = 0; i < nfields_; i++) {
    auto min_size = dser.read<uint64_t>();
    fragment_min_[i].resize(min_size);
    dser.read(fragment_min_[i].data(), min_size);

    auto max_size = dser.read<uint64_t>();
    fragment_max_[i].resize(max_size);
    dser.read(fragment_max_[i].data(), max_size);

    fragment_sum_[i] = dser.read<uint64_t>();
    fragment_null_count_[i] = dser.read<uint64_t>();
  }
}

void FragmentMetadata::dump() {
  footer_.dump();

  fprintf(stderr, "RTree Tile:\n");
  rtree_tile_.dump();

  fprintf(stderr, "Tile Offsets:\n");
  for (size_t i = 0; i < tile_offsets_.size(); i++) {
    fprintf(stderr, "    %zu: %zu offsets\n", i, tile_offsets_[i].size());
    for (auto& offset : tile_offsets_[i]) {
      fprintf(stderr, "        %llu\n", offset);
    }
  }

  fprintf(stderr, "Tile Var Offsets:\n");
  for (size_t i = 0; i < tile_var_offsets_.size(); i++) {
    fprintf(stderr, "    %zu: %zu offsets\n", i, tile_var_offsets_[i].size());
    for (auto& offset : tile_var_offsets_[i]) {
      fprintf(stderr, "        %llu\n", offset);
    }
  }

  fprintf(stderr, "Tile Var Sizes:\n");
  for (size_t i = 0; i < tile_var_sizes_.size(); i++) {
    fprintf(stderr, "    %zu: %zu sizes\n", i, tile_var_sizes_[i].size());
    for (auto& size : tile_var_sizes_[i]) {
      fprintf(stderr, "        %llu\n", size);
    }
  }

  fprintf(stderr, "Tile Validity Offsets:\n");
  for (size_t i = 0; i < tile_validity_offsets_.size(); i++) {
    fprintf(
        stderr, "    %zu: %zu offsets\n", i, tile_validity_offsets_[i].size());
    for (auto& offset : tile_validity_offsets_[i]) {
      fprintf(stderr, "        %llu\n", offset);
    }
  }

  fprintf(stderr, "Tile Min Values:\n");
  for (size_t i = 0; i < tile_min_.size(); i++) {
    fprintf(
        stderr,
        "    %zu: %zu data bytes, %zu var data bytes\n",
        i,
        tile_min_[i].size(),
        tile_min_var_[i].size());
  }

  fprintf(stderr, "Tile Max Values:\n");
  for (size_t i = 0; i < tile_max_.size(); i++) {
    fprintf(
        stderr,
        "    %zu: %zu data bytes, %zu var data bytes\n",
        i,
        tile_max_[i].size(),
        tile_max_var_[i].size());
  }

  fprintf(stderr, "Tile Sums:\n");
  for (size_t i = 0; i < tile_sum_.size(); i++) {
    fprintf(stderr, "    %zu: %zu sum bytes\n", i, tile_sum_[i].size());
  }

  fprintf(stderr, "Tile Null Counts:\n");
  for (size_t i = 0; i < tile_null_count_.size(); i++) {
    fprintf(
        stderr, "    %zu: %lu null counts\n", i, tile_null_count_[i].size());
    for (auto& null_count : tile_null_count_[i]) {
      fprintf(stderr, "        %llu\n", null_count);
    }
  }

  fprintf(stderr, "Fragment Min Value:\n");
  for (size_t i = 0; i < fragment_min_.size(); i++) {
    fprintf(stderr, "    %zu: %zu bytes\n", i, fragment_min_.size());
  }

  fprintf(stderr, "Fragment Max Value:\n");
  for (size_t i = 0; i < fragment_max_.size(); i++) {
    fprintf(stderr, "    %zu: %zu bytes\n", i, fragment_max_.size());
  }

  fprintf(stderr, "Fragment Sums:\n");
  for (size_t i = 0; i < fragment_sum_.size(); i++) {
    fprintf(stderr, "    %zu: %llu\n", i, fragment_sum_[i]);
  }

  fprintf(stderr, "Fragment Null Counts:\n");
  for (size_t i = 0; i < fragment_null_count_.size(); i++) {
    fprintf(stderr, "    %zu: %llu\n", i, fragment_null_count_[i]);
  }
}
