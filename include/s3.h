#pragma once

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>

struct S3Initializer {
  S3Initializer();
  ~S3Initializer();

  Aws::SDKOptions options_;
};

struct S3 {
  S3();
  ~S3();

  std::vector<std::string> ls(const std::string& uri);
  std::vector<uint8_t> read(const std::string& uri);

  Aws::S3::S3Client client_;
};
