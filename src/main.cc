
#include <stdio.h>
#include <unistd.h>
#include <iostream>

#include "fragment_metadata.h"
#include "reader.h"
#include "s3.h"
#include "schema.h"

// Hard coded values from the array schema
#define NUM_FIELDS 15

void verify_last_tile(S3& s3, const Schema& schema, const FragmentMetadata& fmd, const S3Entry& entry);

int main(int argc, char* argv[]) {
  S3Initializer s3ctx;

  // Hard coding the schema because I'm too lazy to write the deserializer
  // for it and all the other things below it right now.
  Schema schema(
      {Dimension("_X", Datatype::FLOAT64), Dimension("_Y", Datatype::FLOAT64)},
      {Attribute("id", Datatype::STRING_UTF8, UINT32_MAX, false),
       Attribute("updatetime", Datatype::STRING_UTF8, UINT32_MAX, false),
       Attribute("version", Datatype::INT32, 1, false),
       Attribute("names", Datatype::STRING_UTF8, UINT32_MAX, false),
       Attribute("level", Datatype::INT32, 1, false),
       Attribute("height", Datatype::FLOAT64, 1, false),
       Attribute("numfloors", Datatype::INT32, 1, false),
       Attribute("class", Datatype::STRING_UTF8, UINT32_MAX, false),
       Attribute("sources", Datatype::STRING_UTF8, UINT32_MAX, false),
       Attribute("bbox", Datatype::FLOAT64, 4, false),
       Attribute("geometry", Datatype::BLOB, UINT32_MAX, false)},
      250000,
      true,
      true,
      true);

  if (argc != 2) {
    fprintf(stderr, "usage: %s ARRAY_URI\n", argv[0]);
    exit(1);
  }

  std::string array_uri(argv[1]);
  std::cerr << "Checking last tile sizes for: " << array_uri << std::endl;

  auto fragments_dir = array_uri + "/__fragments/";

  S3 s3;

  auto fragments = s3.ls(fragments_dir);
  for (auto& fragment : fragments) {
    if (!fragment.is_dir_) {
      std::cerr << "Skipping object: " << fragment.uri_ << std::endl;
    }

    std::cerr << "Checking fragment: " << fragment.uri_ << std::endl;
    auto fragment_files = s3.ls(fragment.uri_);

    S3Entry fmd_entry;
    for (auto& file : fragment_files) {
      if (file.uri_.find("__fragment_metadata.tdb") != std::string::npos) {
        fmd_entry = file;
      }
    }

    if (fmd_entry.uri_.empty()) {
      std::cerr << "Unable to find fragment metadata for: " << fragment.uri_
                << std::endl;
      continue;
    }

    auto data = s3.read(fmd_entry.uri_, fmd_entry.size_, 0);

    Reader reader(data);
    FragmentMetadata fmd(reader, NUM_FIELDS);

    for (auto& file : fragment_files) {
      if (file.uri_.find("__fragment_metadata.tdb") != std::string::npos) {
        continue;
      }

      verify_last_tile(s3, schema, fmd, file);
    }
  }
}

struct FieldInfo {
  FieldInfo(const std::string& uri)
      : uri_(uri) {
    decode_uri();
  }

  void decode_uri() {
    // Get last path part.
    auto slash_pos = uri_.rfind("/");
    if (slash_pos == std::string::npos) {
      throw std::runtime_error("Invalid uri contains no slashes: " + uri_);
    }

    auto filename = uri_.substr(slash_pos + 1, uri_.size() - slash_pos - 1);

    if (filename.front() == 'a') {
      ftype_ = FieldType::ATTR;
    } else if(filename.front() == 'd') {
      ftype_ = FieldType::DIM;
    } else if (filename.front() == 't') {
      ftype_ = FieldType::TIMESTAMPS;
    } else {
      throw std::runtime_error("Unknown file type: " + filename);
    }

    if (ftype_ == FieldType::ATTR || ftype_ == FieldType::DIM) {
      auto underscore_pos = filename.find("_");
      auto dot_pos = filename.find(".");
      if (underscore_pos == std::string::npos && dot_pos == std::string::npos) {
        throw std::runtime_error("Can't find end of digits in: " + filename);
      }
      auto end_digits = underscore_pos;
      if (end_digits == std::string::npos) {
        end_digits = dot_pos;
      }
      auto digits = filename.substr(1, end_digits - 1);
      std::stringstream ss;
      ss << digits;
      ss >> type_index_;

      if (underscore_pos != std::string::npos) {
        var_data_ = (filename.find("_var") != std::string::npos);
        validity_data_ = (filename.find("_validity") != std::string::npos);
      } else {
        var_data_ = false;
        validity_data_ = false;
      }
    }
  }

  std::string uri_;
  FieldType ftype_;
  uint64_t type_index_;
  bool var_data_;
  bool validity_data_;
};

uint64_t
calculate_expected_size(const Schema& schema, const FragmentMetadata& fmd, const FieldInfo& fi) {
  auto field_index = schema.field_index(fi.ftype_, fi.type_index_);

  // Var data is recorded in the fragment data
  if (fi.var_data_) {
    return fmd.tile_var_sizes_[field_index].back();
  }

  auto cell_count = fmd.footer_.last_tile_cell_num_;

  if (schema.is_var_sized(fi.ftype_, fi.type_index_)) {
    // This is offsets data
    return cell_count * sizeof(uint64_t);
  }

  // Fixed sized data, so cell count * cell size
  return cell_count * schema.cell_size(fi.ftype_, fi.type_index_);
}

std::tuple<uint64_t, uint64_t>
get_object_read_info(const Schema& schema, const FragmentMetadata& fmd, const FieldInfo& fi) {
  auto field_index = schema.field_index(fi.ftype_, fi.type_index_);

  if (fi.var_data_) {
    auto offset = fmd.tile_var_offsets_[field_index].back();
    auto size = fmd.footer_.file_var_sizes_[field_index];
    return {offset, size - offset};
  } else {
    auto offset = fmd.tile_offsets_[field_index].back();
    auto size = fmd.footer_.file_sizes_[field_index];
    return {offset, size - offset};
  }
}

void
verify_last_tile(S3& s3, const Schema& schema, const FragmentMetadata& fmd, const S3Entry& entry) {
  std::cerr << "CHECKING: " << entry.uri_ << std::endl;

  FieldInfo fi(entry.uri_);

  // Skip timestamp verification until I know what they are data size wise.
  if (fi.ftype_ == FieldType::TIMESTAMPS) {
    return;
  }

  auto expected_size = calculate_expected_size(schema, fmd, fi);
  auto [offset, length] = get_object_read_info(schema, fmd, fi);
  auto data = s3.read(entry.uri_, length, offset);

  ChunkData chunks(data.data(), data.size());

  if (chunks.orig_size_ != expected_size) {
    std::cerr << "INVALID:" << std::endl;
    std::cerr << "    Expected:       " << expected_size << " bytes" << std::endl;
    std::cerr << "    Last Tile Size: " << chunks.orig_size_ << " bytes" <<std::endl;
  }
}
