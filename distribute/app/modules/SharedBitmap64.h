#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include "vendor/roaring/roaring.hh"

// Immutable row sharing, with detachment before mutation. The shared_ptr
// control block supports ownership crossing the existing RPC/task queues;
// it does not introduce compute threads or share mutable row contents.
class SharedBitmap64 {
    using Bitmap = roaring::Roaring64Map;
    std::shared_ptr<Bitmap> value_;
    uint64_t cardinality_ = 0;

    Bitmap& writable() {
        if (!value_) {
            value_ = std::make_shared<Bitmap>();
            cardinality_ = 0;
        }
        else if (!value_.unique()) value_ = std::make_shared<Bitmap>(*value_);
        return *value_;
    }

public:
    SharedBitmap64() = default;
    SharedBitmap64(const Bitmap& value)
        : value_(std::make_shared<Bitmap>(value)), cardinality_(value_->cardinality()) {}
    SharedBitmap64(Bitmap&& value)
        : value_(std::make_shared<Bitmap>(std::move(value))), cardinality_(value_->cardinality()) {}

    const Bitmap& view() const {
        static const Bitmap empty;
        return value_ ? *value_ : empty;
    }
    const void* identity() const { return value_.get(); }
    bool isEmpty() const { return cardinality() == 0; }
    uint64_t cardinality() const { return value_ ? cardinality_ : 0; }
    auto begin() const { return view().begin(); }
    auto end() const { return view().end(); }
    void add(uint64_t value) { addChecked(value); }
    bool addChecked(uint64_t value) {
        const bool added = writable().addChecked(value);
        cardinality_ += added;
        return added;
    }
    bool contains(uint64_t value) const { return view().contains(value); }
    bool select(uint64_t rank, uint64_t* value) const { return view().select(rank, value); }
    uint64_t minimum() const { return view().minimum(); }
    uint64_t maximum() const { return view().maximum(); }
    size_t getSizeInBytes(bool portable = true) const { return view().getSizeInBytes(portable); }
    size_t write(char* output, bool portable = true) const { return view().write(output, portable); }
    bool isSubset(const SharedBitmap64& other) const {
        return value_ == other.value_ || view().isSubset(other.view());
    }

    SharedBitmap64& operator|=(const SharedBitmap64& other) {
        if (value_ == other.value_ || other.isEmpty()) return *this;
        if (isEmpty()) *this = other;
        else {
            if (value_.unique()) *value_ |= other.view();
            else value_ = std::make_shared<Bitmap>(view() | other.view());
            cardinality_ = view().cardinality();
        }
        return *this;
    }
    SharedBitmap64& operator&=(const SharedBitmap64& other) {
        if (value_ == other.value_) return *this;
        if (other.isEmpty()) {
            value_.reset();
            cardinality_ = 0;
        } else if (!isEmpty()) {
            writable() &= other.view();
            cardinality_ = view().cardinality();
        }
        return *this;
    }
    SharedBitmap64& operator-=(const SharedBitmap64& other) {
        if (value_ == other.value_) {
            value_.reset();
            cardinality_ = 0;
        } else if (!isEmpty() && !other.isEmpty()) {
            writable() -= other.view();
            cardinality_ = view().cardinality();
        }
        return *this;
    }
    SharedBitmap64& operator^=(const SharedBitmap64& other) {
        if (value_ == other.value_) {
            value_.reset();
            cardinality_ = 0;
        }
        else if (isEmpty()) *this = other;
        else if (!other.isEmpty()) {
            writable() ^= other.view();
            cardinality_ = view().cardinality();
        }
        return *this;
    }
    friend SharedBitmap64 operator|(const SharedBitmap64& left, const SharedBitmap64& right) {
        if (left.value_ == right.value_ || right.isEmpty()) return left;
        if (left.isEmpty()) return right;
        return left.view() | right.view();
    }
    friend SharedBitmap64 operator&(SharedBitmap64 left, const SharedBitmap64& right) {
        left &= right;
        return left;
    }
    friend SharedBitmap64 operator-(SharedBitmap64 left, const SharedBitmap64& right) {
        left -= right;
        return left;
    }
    friend SharedBitmap64 operator^(SharedBitmap64 left, const SharedBitmap64& right) {
        left ^= right;
        return left;
    }
    bool operator==(const SharedBitmap64& other) const {
        return value_ == other.value_ || view() == other.view();
    }
    bool operator!=(const SharedBitmap64& other) const { return !(*this == other); }
};
