
#include <zlib.h>

#include "decompressor.h"
#include "deserializer.h"

void
decompress_part(uint8_t* src, size_t src_nbytes, uint8_t* dst, size_t dst_nbytes) {
  z_stream strm;
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  strm.avail_in = 0;
  strm.next_in = Z_NULL;

  if (inflateInit(&strm) != Z_OK) {
    fprintf(stderr, "Failed to initialize decompression stream.\n");
    exit(2);
  }

  strm.next_in = src;
  strm.next_out = dst;
  strm.avail_in = src_nbytes;
  strm.avail_out = dst_nbytes;

  if (inflate(&strm, Z_FINISH) != Z_STREAM_END) {
    fprintf(stderr, "Failed to decompress buffer.\n");
    exit(2);
  }

  (void)inflateEnd(&strm);
}


void
tdb_decompress(DiskLayout& layout, uint8_t* buf, size_t nbytes)
{
  Deserializer dser(layout.filtered_metadata_, layout.filtered_metadata_size_);
  auto num_metadata_parts = dser.read<uint32_t>();
  auto num_data_parts = dser.read<uint32_t>();

  //fprintf(stderr, "Decompression %d metadata parts, %d data parts.\n", num_metadata_parts, num_data_parts);

  if (num_metadata_parts != 0) {
    fprintf(stderr, "Found metadata parts in gzip decompressor.");
    exit(2);
  }

  // Setup references to our buffers that can be moved as we work through
  // decompressing the chunks.
  uint8_t* curr_src = layout.filtered_data_;
  uint8_t* curr_dst = buf;

  size_t src_bytes = layout.filtered_data_size_;
  size_t dst_bytes = nbytes;

  for (size_t i = 0; i < num_data_parts; i++) {
    auto uncompressed_size = dser.read<uint32_t>();
    auto compressed_size = dser.read<uint32_t>();

    if (compressed_size > src_bytes) {
      fprintf(stderr, "Error decompression chunk, not enough input buffer.\n");
      exit(2);
    }

    if (uncompressed_size > dst_bytes) {
      fprintf(stderr, "Error dcompression chunk, not enough output buffer.\n");
      exit(2);
    }

    //fprintf(stderr, "Decompressing data chunk from %u to %u bytes.\n", compressed_size, uncompressed_size);
    decompress_part(curr_src, compressed_size, curr_dst, uncompressed_size);
  }
}
