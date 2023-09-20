
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>

#include "s3.h"

std::string add_front_slash(const std::string& path);
std::string remove_front_slash(const std::string& path);
std::string remove_trailing_slash(const std::string& path);

template <typename R, typename E>
std::string s3_error_message(const Aws::Utils::Outcome<R, E>& outcome);

struct PreallocatedIOStream : public Aws::IOStream {
  PreallocatedIOStream(uint8_t* buf, size_t size)
      : Aws::IOStream(
            new Aws::Utils::Stream::PreallocatedStreamBuf(buf, size)) {
  }

  /** Destructor. */
  ~PreallocatedIOStream() {
    // Delete the unmanaged `Aws::Utils::Stream::PreallocatedStreamBuf`
    // that was allocated in the constructor.
    delete rdbuf();
  }
};

S3Initializer::S3Initializer() {
  // Optionally change the log level for debugging.
  // options.loggingOptions.logLevel = Utils::Logging::LogLevel::Debug;
  Aws::InitAPI(options_);
}

S3Initializer::~S3Initializer() {
  Aws::ShutdownAPI(options_);
}

S3::S3() {
  Aws::Client::ClientConfiguration config;
  auto provider =
      Aws::MakeShared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(
          "fmd");
  auto creds = provider->GetAWSCredentials();
  if (creds.IsEmpty()) {
    throw std::runtime_error("Failed to authenticate to S3.");
  }

  client_ = Aws::S3::S3Client(config);
}

std::vector<S3Entry> S3::ls(const std::string& uri) {
  auto s3_uri = remove_trailing_slash(uri);
  Aws::Http::URI aws_uri = s3_uri.c_str();

  std::string bucket = aws_uri.GetAuthority().c_str();
  std::string prefix = remove_front_slash(aws_uri.GetPath().c_str());

  Aws::S3::Model::ListObjectsV2Request req;
  req.SetBucket(aws_uri.GetAuthority());
  req.SetPrefix(prefix.c_str());
  req.SetDelimiter("/");

  std::vector<S3Entry> ret;

  while (true) {
    auto outcome = client_.ListObjectsV2(req);
    if (!outcome.IsSuccess()) {
      auto msg = s3_error_message(outcome);
      throw std::runtime_error("Failed to list bucket: '" + uri + "' : " + msg);
    }

    Aws::Vector<Aws::S3::Model::Object> objects =
        outcome.GetResult().GetContents();
    for (Aws::S3::Model::Object& object : objects) {
      std::string file(object.GetKey().c_str());
      uint64_t size = object.GetSize();
      ret.emplace_back("s3://" + bucket + add_front_slash(file), size, false);
    }

    for (const auto& object : outcome.GetResult().GetCommonPrefixes()) {
      std::string file(object.GetPrefix().c_str());
      file = "s3://" + bucket + add_front_slash(remove_trailing_slash(file));
      ret.emplace_back(file, 0, true);
    }

    if (!outcome.GetResult().GetIsTruncated()) {
      break;
    }

    Aws::String token = outcome.GetResult().GetNextContinuationToken();
    if (token.empty()) {
      throw std::runtime_error(
          "Broken ListObjectsV2 request missing "
          "it's continuation token.");
    }

    req.SetContinuationToken(std::move(token));
  }

  return ret;
}

std::vector<uint8_t> S3::read(
    const std::string& uri, size_t offset, size_t nbytes) {
  Aws::Http::URI aws_uri = uri.c_str();

  auto range = "bytes=" + std::to_string(offset) + "-" +
               std::to_string(offset + nbytes - 1);

  std::vector<uint8_t> ret(nbytes);

  Aws::S3::Model::GetObjectRequest req;
  req.WithBucket(aws_uri.GetAuthority());
  req.WithKey(aws_uri.GetPath());
  req.SetRange(range.c_str());
  req.SetResponseStreamFactory([&ret]() {
    return Aws::New<PreallocatedIOStream>("fmd", ret.data(), ret.size());
  });

  auto outcome = client_.GetObject(req);
  if (!outcome.IsSuccess()) {
    auto msg = s3_error_message(outcome);
    throw std::runtime_error("Failed to get object: '" + uri + "' : " + msg);
  }

  auto nread = static_cast<uint64_t>(outcome.GetResult().GetContentLength());
  if (nread != nbytes) {
    auto msg = "Error reading S3 object '" + uri + "' : Expected " +
               std::to_string(nbytes) + " bytes but read " +
               std::to_string(nread) + " bytes.";
    throw std::runtime_error(msg);
  }

  return ret;
}

std::string add_front_slash(const std::string& path) {
  if (path.front() == '/') {
    return path;
  } else {
    return "/" + path;
  }
}

std::string remove_front_slash(const std::string& path) {
  if (path.front() == '/') {
    return path.substr(1, path.size() - 1);
  } else {
    return path;
  }
}

std::string remove_trailing_slash(const std::string& path) {
  if (path.back() == '/') {
    return path.substr(1, path.size() - 1);
  } else {
    return path;
  }
}

template <typename R, typename E>
std::string s3_error_message(const Aws::Utils::Outcome<R, E>& outcome) {
  if (outcome.IsSuccess()) {
    return "Success";
  }

  auto err = outcome.GetError();
  Aws::StringStream ss;

  ss << "[Error Type: " << static_cast<int>(err.GetErrorType()) << "]"
     << " [HTTP Response Code: " << static_cast<int>(err.GetResponseCode())
     << "]";

  if (!err.GetExceptionName().empty()) {
    ss << " [Exception: " << err.GetExceptionName() << "]";
  }

  if (!err.GetRemoteHostIpAddress().empty()) {
    ss << " [Remote IP: " << err.GetRemoteHostIpAddress() << "]";
  }

  if (!err.GetRequestId().empty()) {
    ss << " [Request ID: " << err.GetRequestId() << "]";
  }

  if (err.GetResponseHeaders().size() > 0) {
    ss << " [Headers:";
    for (auto&& h : err.GetResponseHeaders()) {
      ss << " '" << h.first << "' = '" << h.second << "'";
    }
    ss << "]";
  }

  ss << " : " << err.GetMessage();

  return ss.str();
}
