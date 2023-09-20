#pragma once

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>

struct S3Initializer {
  S3Initializer();
  ~S3Initializer();

  Aws::SDKOptions options_;
};

struct S3Entry {
  S3Entry(const std::string& uri, uint64_t size, bool is_dir)
      : uri_(uri)
      , size_(size)
      , is_dir_(is_dir) {
  }

  std::string uri_;
  uint64_t size_;
  bool is_dir_;
};

struct S3 {
  S3();

  std::vector<S3Entry> ls(const std::string& uri);
  std::vector<uint8_t> read(
      const std::string& uri, size_t offset, size_t nbytes);

  Aws::S3::S3Client client_;
};
