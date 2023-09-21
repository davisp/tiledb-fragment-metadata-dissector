#pragma once

enum struct Datatype {
  INT32 = 0,
  INT64 = 1,
  FLOAT32 = 2,
  FLOAT64 = 3,
  CHAR = 4,
  INT8 = 5,
  UINT8 = 6,
  INT16 = 7,
  UINT16 = 8,
  UINT32 = 9,
  UINT64 = 10,

  STRING_ASCII = 11,
  STRING_UTF8 = 12,
  STRING_UTF16 = 13,
  STRING_UTF32 = 14,
  STRING_UCS2 = 15,
  STRING_UCS4 = 16,

  ANY = 17,

  DATETIME_YEAR = 18,
  DATETIME_MONTH = 19,
  DATETIME_WEEK = 20,
  DATETIME_DAY = 21,
  DATETIME_HR = 22,
  DATETIME_MIN = 23,
  DATETIME_SEC = 24,
  DATETIME_MS = 25,
  DATETIME_US = 26,
  DATETIME_NS = 27,
  DATETIME_PS = 28,
  DATETIME_FS = 29,
  DATETIME_AS = 30,

  TIME_HR = 31,
  TIME_MIN = 32,
  TIME_SEC = 33,
  TIME_MS = 34,
  TIME_US = 35,
  TIME_NS = 36,
  TIME_PS = 37,
  TIME_FS = 38,
  TIME_AS = 39,

  BLOB = 40,
  BOOL = 41,

  INVALID = 255,
};

uint64_t datatype_size(Datatype dtype) {
  switch (dtype) {
    case Datatype::INT32: {
      return sizeof(int32_t);
    }
    case Datatype::INT64: {
      return sizeof(int64_t);
    }
    case Datatype::FLOAT32: {
      return sizeof(float);
    }
    case Datatype::FLOAT64: {
      return sizeof(double);
    }
    case Datatype::CHAR: {
      return sizeof(char);
    }
    case Datatype::INT8: {
      return sizeof(int8_t);
    }
    case Datatype::UINT8: {
      return sizeof(uint8_t);
    }
    case Datatype::INT16: {
      return sizeof(int16_t);
    }
    case Datatype::UINT16: {
      return sizeof(uint16_t);
    }
    case Datatype::UINT32: {
      return sizeof(uint32_t);
    }
    case Datatype::UINT64: {
      return sizeof(uint64_t);
    }
    case Datatype::STRING_ASCII: {
      return sizeof(char);
    }
    case Datatype::STRING_UTF8: {
      return sizeof(uint8_t);
    }
    case Datatype::STRING_UTF16: {
      return sizeof(uint16_t);
    }
    case Datatype::STRING_UTF32: {
      return sizeof(uint32_t);
    }
    case Datatype::STRING_UCS2: {
      return sizeof(uint16_t);
    }
    case Datatype::STRING_UCS4: {
      return sizeof(uint32_t);
    }
    case Datatype::ANY: {
      return sizeof(std::byte);
    }
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS: {
      return sizeof(uint64_t);
    }
    case Datatype::BLOB: {
      return sizeof(std::byte);
    }
    case Datatype::BOOL: {
      return sizeof(uint8_t);
    }
    default: {
      auto val = static_cast<uint8_t>(dtype);
      throw std::invalid_argument("Invalid datatype: " + std::to_string(val));
    }
  }
}
