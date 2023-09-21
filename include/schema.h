#pragma once

#include "datatype.h"

enum class FieldType {
  ATTR = 1,
  COORDS = 2,
  DIM = 3,
  TIMESTAMPS = 4,
};

struct Dimension {
  Dimension()
      : dtype_(Datatype::INVALID) {
  }
  Dimension(std::string name, Datatype dtype)
      : name_(name)
      , dtype_(dtype) {
  }

  std::string name_;
  Datatype dtype_;
};

struct Attribute {
  Attribute()
      : dtype_(Datatype::INVALID)
      , cell_val_num_(0)
      , nullable_(false) {
  }
  Attribute(
      std::string name, Datatype dtype, uint32_t cell_val_num, bool nullable)
      : name_(name)
      , dtype_(dtype)
      , cell_val_num_(cell_val_num)
      , nullable_(nullable) {
  }

  std::string name_;
  Datatype dtype_;
  uint32_t cell_val_num_;
  bool nullable_;
};

struct Schema {
  Schema()
      : capacity_(0)
      , sparse_(false)
      , allow_dups_(false)
      , has_timestamps_(false) {
  }
  Schema(
      std::vector<Dimension> dims,
      std::vector<Attribute> attrs,
      uint64_t capacity,
      bool sparse,
      bool allow_dups,
      bool has_timestamps)
      : dims_(dims)
      , attrs_(attrs)
      , capacity_(capacity)
      , sparse_(sparse)
      , allow_dups_(allow_dups)
      , has_timestamps_(has_timestamps) {
  }

  size_t field_index(FieldType ftype, size_t field_idx) const {
    if(ftype == FieldType::ATTR) {
      return field_idx;
    }

    if (ftype == FieldType::COORDS) {
      return attrs_.size();
    }

    if (ftype == FieldType::DIM) {
      return attrs_.size() + 1 + field_idx;
    }

    if (ftype == FieldType::TIMESTAMPS) {
      return attrs_.size() + 1 + dims_.size() + 1;
    }

    throw std::invalid_argument("Unknown field type.");
  }

  bool is_var_sized(FieldType ftype, size_t field_idx) const {
    if (ftype != FieldType::ATTR) {
      return false;
    }

    return attrs_[field_idx].cell_val_num_ == UINT32_MAX;
  }

  size_t cell_size(FieldType ftype, size_t field_idx) const {
    if (ftype == FieldType::ATTR) {
      if (attrs_[field_idx].cell_val_num_ == UINT32_MAX) {
        throw std::invalid_argument("No cell size for var sized attributes.");
      }

      return attrs_[field_idx].cell_val_num_ * datatype_size(attrs_[field_idx].dtype_);
    }

    if (ftype == FieldType::DIM) {
      return datatype_size(dims_[field_idx].dtype_);
    }

    throw std::invalid_argument("Invalid field type for cell_size");
  }

  std::vector<Dimension> dims_;
  std::vector<Attribute> attrs_;

  uint64_t capacity_;
  bool sparse_;
  bool allow_dups_;
  bool has_timestamps_;
};
