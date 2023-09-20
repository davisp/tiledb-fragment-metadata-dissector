
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <iostream>
#include <sstream>

#include "reader.h"

struct Fd {
  Fd(const char* filename) {
    fd_ = ::open(filename, O_RDONLY);
    if (fd_ < 0) {
      std::stringstream ss;
      ss << "Error opening '" << filename << "' : " << strerror(errno);
      throw std::runtime_error(ss.str());
    }
  }

  ~Fd() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  int fd_;
};

Reader::Reader(const char* filename) {
  Fd fd(filename);

  auto size = lseek(fd.fd_, 0, SEEK_END);
  data_.resize(size);

  auto nread = ::pread(fd.fd_, data_.data(), size, 0);
  if (nread < 0) {
    throw std::runtime_error(std::string("Error in pread: ") + strerror(errno));
  }

  if (nread != size) {
    std::stringstream ss;
    ss << "Read failed: Expected " << size << " bytes, ";
    ss << "but read " << nread << "bytes.";
    throw std::runtime_error(ss.str());
  }

  std::cerr << "Read " << nread << " bytes." << std::endl;

  read_map_.resize(size, 0);
}

Reader::Reader(const std::vector<uint8_t> data)
    : data_(data)
    , read_map_(data_.size(), 0) {
}

size_t Reader::size() {
  return data_.size();
}

void Reader::read(void* buf, size_t nbytes, size_t offset) {
  if (nbytes + offset > data_.size()) {
    throw std::runtime_error("Error reading past end of file.");
  }

  std::memcpy(buf, data_.data() + offset, nbytes);
  for (size_t i = offset; i < offset + nbytes; i++) {
    read_map_[i] += 1;
  }
}

void Reader::show_read_report() {
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
