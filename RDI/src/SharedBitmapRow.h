#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include "roaring/roaring.hh"

// Single-thread ownership for immutable rows, not CRoaring's COW containers.
// Mutations still detach shared values. This type is not thread-safe.
class SharedBitmapRow {
    struct Value {
        size_t references = 1;
        roaring::Roaring bitmap;
        uint64_t cardinality;
        explicit Value(const roaring::Roaring& row)
            : bitmap(row), cardinality(bitmap.cardinality()) {}
        explicit Value(roaring::Roaring&& row)
            : bitmap(std::move(row)), cardinality(bitmap.cardinality()) {}
    };

public:
    SharedBitmapRow() = default;
    SharedBitmapRow(const roaring::Roaring& value)
        : value_(new Value(value)) {}
    SharedBitmapRow(roaring::Roaring&& value)
        : value_(new Value(std::move(value))) {}
    SharedBitmapRow(const SharedBitmapRow& other) noexcept : value_(other.value_) {
        if (value_) ++value_->references;
    }
    SharedBitmapRow(SharedBitmapRow&& other) noexcept
        : value_(std::exchange(other.value_, nullptr)) {}
    SharedBitmapRow& operator=(const SharedBitmapRow& other) noexcept {
        if (value_ != other.value_) {
            auto* next = other.value_;
            if (next) ++next->references;
            release();
            value_ = next;
        }
        return *this;
    }
    SharedBitmapRow& operator=(SharedBitmapRow&& other) noexcept {
        if (this != &other) {
            release();
            value_ = std::exchange(other.value_, nullptr);
        }
        return *this;
    }
    ~SharedBitmapRow() { release(); }

    const roaring::Roaring& bitmap() const {
        static const roaring::Roaring empty;
        return value_ ? value_->bitmap : empty;
    }
    operator const roaring::Roaring&() const { return bitmap(); }
    bool isEmpty() const { return cardinality() == 0; }
    uint64_t cardinality() const { return value_ ? value_->cardinality : 0; }
    const void* storage_identity() const noexcept { return value_; }
    uint64_t and_cardinality(const roaring::Roaring& rhs) const {
        return bitmap().and_cardinality(rhs);
    }
    size_t getSizeInBytes() const { return bitmap().getSizeInBytes(); }
    bool contains(uint32_t value) const { return bitmap().contains(value); }
    auto begin() const { return bitmap().begin(); }
    auto end() const { return bitmap().end(); }
    void add(uint32_t value) {
        auto& row = mutable_value();
        row.cardinality += row.bitmap.addChecked(value);
    }
    SharedBitmapRow& operator|=(const SharedBitmapRow& rhs) {
        if (isEmpty()) *this = rhs;
        else if (value_ != rhs.value_) *this |= rhs.bitmap();
        return *this;
    }
    SharedBitmapRow& operator|=(const roaring::Roaring& rhs) {
        if (rhs.isEmpty()) return *this;
        if (!value_ || value_->references != 1) {
            auto* next = new Value(bitmap() | rhs);
            release();
            value_ = next;
        } else {
            value_->bitmap |= rhs;
            value_->cardinality = value_->bitmap.cardinality();
        }
        return *this;
    }
    roaring::Roaring operator&(const roaring::Roaring& rhs) const {
        return bitmap() & rhs;
    }
    SharedBitmapRow operator-(const SharedBitmapRow& rhs) const {
        return bitmap() - rhs.bitmap();
    }

private:
    void release() noexcept {
        if (value_ && --value_->references == 0) delete value_;
    }
    Value& mutable_value() {
        if (!value_) value_ = new Value(roaring::Roaring{});
        else if (value_->references != 1) {
            auto* next = new Value(value_->bitmap);
            release();
            value_ = next;
        }
        return *value_;
    }
    Value* value_ = nullptr;
};
