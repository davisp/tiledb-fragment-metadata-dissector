
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "reader.h"

Reader::Reader(const char* filename) {
  fd_ = ::open(filename, O_RDONLY);
  if (fd_ < 0) {
    fprintf(stderr, "Error opening '%s': %s", filename, strerror(errno));
    exit(2);
  }

  file_size_ = lseek(fd_, 0, SEEK_END);
  read_map_.resize(file_size_, 0);
}

void
Reader::read(void* buf, size_t nbytes, size_t offset)
{
  //fprintf(stderr, "Reading %lu bytes at %zu offset.\n", nbytes, offset);

  auto nread = pread(fd_, buf, nbytes, offset);
  if (nread < 0) {
    fprintf(stderr, "Error in pread: %s\n", strerror(errno));
    exit(2);
  }

  if (nread != nbytes) {
    auto fmt = "Read failed: Expected %ld bytes, "
      "but read %ld bytes\n";
    fprintf(stderr, fmt, nbytes, nread);
    exit(2);
  }

  for (size_t i = offset; i < offset + nbytes; i++) {
    read_map_[i] += 1;
  }
}

void
Reader::show_read_report() {
  bool found_hole = false;
  for (size_t i = 0; i < read_map_.size(); i++) {
    if (read_map_[i] > 0) {
      continue;
    }

    if (found_hole == false) {
      fprintf(stderr, "Found unread bytes in metadata file:\n");
    }
    found_hole = true;

    size_t j = i;
    while (j < read_map_.size() && read_map_[j] == 0) {
      j++;
    }

    fprintf(stderr, "    %zu - %zu\n", i, j);
    i = j;
  }

  if (!found_hole) {
    fprintf(stderr, "Metadata file was read completely.\n");
  }
}
