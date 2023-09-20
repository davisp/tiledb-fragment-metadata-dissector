#include <stdio.h>

#include "decompressor.h"
#include "deserializer.h"
#include "reader.h"
#include "tile.h"

struct ChunkData {
  ChunkData(uint8_t* buf, size_t nbytes);

  size_t size() {
    return filtered_chunks_.size();
  }

  void dump();

  std::vector<DiskLayout> filtered_chunks_;
  uint64_t orig_size;
};

struct Header {
  static const uint64_t BASE_SIZE =
      3 * sizeof(uint64_t) + 2 * sizeof(char) + 2 * sizeof(uint32_t);

  Header()
      : version(0)
      , persisted_size(0)
      , tile_size(0)
      , datatype(255)
      , cell_size(0)
      , encryption_type(255)
      , filter_pipeline_size(0) {
  }

  void dump();

  uint32_t version;
  uint64_t persisted_size;
  uint64_t tile_size;
  uint8_t datatype;
  uint64_t cell_size;
  uint8_t encryption_type;
  uint32_t filter_pipeline_size;
};

DiskLayout::DiskLayout()
    : unfiltered_data_size_(0)
    , unfiltered_data_offset_(0)
    , filtered_data_size_(0)
    , filtered_metadata_size_(0)
    , filtered_metadata_(nullptr)
    , filtered_data_(nullptr) {
}

void DiskLayout::dump() {
  fprintf(stderr, "    DiskLayout\n");
  fprintf(
      stderr,
      "        Unfiltered data: %u bytes at %llu offset\n",
      unfiltered_data_size_,
      unfiltered_data_offset_);
  fprintf(
      stderr, "        Filtered Metadata: %u bytes\n", filtered_metadata_size_);
  fprintf(stderr, "        Filtered Data: %u bytes\n", filtered_data_size_);
}

ChunkData::ChunkData(uint8_t* buf, size_t nbytes) {
  Deserializer deserializer(buf, nbytes);
  uint64_t num_chunks = deserializer.read<uint64_t>();

  filtered_chunks_.resize(num_chunks);

  // fprintf(stderr, "Loading %llu chunks.\n", num_chunks);

  orig_size = 0;
  for (uint64_t i = 0; i < num_chunks; i++) {
    auto& chunk = filtered_chunks_[i];
    chunk.unfiltered_data_size_ = deserializer.read<uint32_t>();
    chunk.unfiltered_data_offset_ = orig_size;
    chunk.filtered_data_size_ = deserializer.read<uint32_t>();
    chunk.filtered_metadata_size_ = deserializer.read<uint32_t>();

    chunk.filtered_metadata_ = const_cast<uint8_t*>(
        deserializer.get_ptr<uint8_t>(chunk.filtered_metadata_size_));

    chunk.filtered_data_ = const_cast<uint8_t*>(
        deserializer.get_ptr<uint8_t>(chunk.filtered_data_size_));

    orig_size += chunk.unfiltered_data_size_;
  }
}

void ChunkData::dump() {
  fprintf(stderr, "ChunkData:\n");
  fprintf(stderr, "    NumChunks: %zu\n", filtered_chunks_.size());
  for (auto& chunk : filtered_chunks_) {
    chunk.dump();
  }
}

void Header::dump() {
  fprintf(stderr, "Tile Header:\n");
  fprintf(stderr, "    Version: %u\n", version);
  fprintf(stderr, "    Persisted Size: %llu\n", persisted_size);
  fprintf(stderr, "    Tile Size: %llu\n", tile_size);
  fprintf(stderr, "    Datatype: %u\n", datatype);
  fprintf(stderr, "    Cell Size: %llu\n", cell_size);
  fprintf(stderr, "    Encryption Type: %u\n", encryption_type);
  fprintf(stderr, "    Filter Pipeline Size: %u\n", filter_pipeline_size);
}

Header read_header(Reader& reader, uint64_t offset) {
  Header header;
  std::vector<uint8_t> buf(Header::BASE_SIZE);
  reader.read(buf.data(), buf.size(), offset);
  Deserializer dser(buf.data(), buf.size());

  header.version = dser.read<uint32_t>();
  header.persisted_size = dser.read<uint64_t>();
  header.tile_size = dser.read<uint64_t>();
  header.datatype = dser.read<uint8_t>();
  header.cell_size = dser.read<uint64_t>();
  header.encryption_type = dser.read<uint8_t>();
  header.filter_pipeline_size = dser.read<uint32_t>();

  // We read the bytes that contain the filter pipeline settings but
  // generic tiles have a single statically defined filter pipeline that
  // I've implemented outside of TileDB core. This read just exists to show
  // where the bytes are and to avoid the Reader::show_read_report from
  // listing each filter pipeline as unread.

  std::vector<uint8_t> fp_buf(header.filter_pipeline_size);
  reader.read(fp_buf.data(), fp_buf.size(), offset + Header::BASE_SIZE);

  return header;
}

Tile read_tile(Reader& reader, uint64_t offset) {
  // fprintf(stderr, "Reading tile at offset: %llu\n", offset);

  auto header = read_header(reader, offset);
  uint64_t data_offset =
      offset + Header::BASE_SIZE + header.filter_pipeline_size;

  std::vector<uint8_t> raw_tile_data(header.persisted_size);
  reader.read(raw_tile_data.data(), raw_tile_data.size(), data_offset);

  ChunkData chunks(raw_tile_data.data(), raw_tile_data.size());

  if (chunks.orig_size != header.tile_size) {
    fprintf(stderr, "Error deserializing tile, header size mismatch.");
    exit(2);
  }

  Tile tile(
      header.version, header.datatype, header.cell_size, header.tile_size);

  for (size_t i = 0; i < chunks.size(); i++) {
    auto& chunk = chunks.filtered_chunks_[i];
    tdb_decompress(chunk, tile.data_.data(), tile.data_.size());
  }

  return tile;
}

void Tile::dump() {
  fprintf(stderr, "    Version: %u\n", version_);
  fprintf(stderr, "    Datatype: %u\n", datatype_);
  fprintf(stderr, "    Cell Size: %llu\n", cell_size_);
  fprintf(stderr, "    Data Size: %lu\n", data_.size());
}
