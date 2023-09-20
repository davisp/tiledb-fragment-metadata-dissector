#pragma once

#include <string.h>

#include <cstdint>
#include <stdexcept>

class Deserializer {
 public:
  /**
   * Deleted default constructor.
   */
  Deserializer() = delete;

  /**
   * Constructor using a preallocated buffer.
   *
   * @param data Preallocated buffer.
   * @param size Size of the buffer.
   */
  Deserializer(const void* data, uint64_t size)
      : ptr_(static_cast<const uint8_t*>(data))
      , size_(size) {
  }

  /**
   * Deserialize fixed size data.
   *
   * @tparam T Type of the data to write.
   * @return Data read.
   */
  template <class T>
  T read() {
    if (sizeof(T) > size_) {
      throw std::logic_error("Reading data past end of serialized data size.");
    }

    T ret;
    memcpy(&ret, ptr_, sizeof(T));
    ptr_ += sizeof(T);
    size_ -= sizeof(T);

    return ret;
  }

  /**
   * Deserialize a buffer.
   *
   * @param data data to read.
   * @param size size of the data.
   */
  void read(void* data, uint64_t size) {
    if (size > size_) {
      throw std::logic_error("Reading data past end of serialized data size.");
    }

    memcpy(data, ptr_, size);
    ptr_ += size;
    size_ -= size;
  }

  /**
   * Return remaining number of bytes to deserialize.
   *
   * @return remaining unread bytes in deserializer.
   */
  uint64_t remaining_bytes() const {
    return size_;
  }

  /**
   * Return a pointer to serialized data.
   *
   * @tparam T Type of the data.
   * @param size Size of the data.
   * @return Pointer to the data.
   */
  template <class T>
  const T* get_ptr(uint64_t size) {
    if (size > size_) {
      throw std::logic_error("Reading data past end of serialized data size.");
    }

    auto ptr = static_cast<const T*>(static_cast<const void*>(ptr_));
    ptr_ += size;
    size_ -= size;

    return ptr;
  }

  /**
   * Return the size left to be read.
   *
   * @return Size left to be read.
   */
  inline uint64_t size() {
    return size_;
  }

 private:
  /* Pointer to the current data to be read. */
  const uint8_t* ptr_;

  /* Size left to be read. */
  uint64_t size_;
};
