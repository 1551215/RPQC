#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <new>
#include <vector>
#include "roaring/roaring.hh"

// Serial scratch only: every operation transfers or frees all owned containers.
// The direct directory is the fixed 16-bit Roaring key domain, not a cutoff.
class FixedBitmapUnionScratch {
    struct Slot {
        roaring::internal::container_t* container = nullptr;
        uint8_t type = 0;
    };
    std::array<Slot, 1ULL << 16> slots_{};
    std::vector<uint16_t> keys_;

    void clear() noexcept {
        for (uint16_t key : keys_) {
            auto& slot = slots_[key];
            if (slot.container) roaring::internal::container_free(slot.container, slot.type);
            slot = {};
        }
        keys_.clear();
    }

public:
    FixedBitmapUnionScratch() { keys_.reserve(slots_.size()); }
    FixedBitmapUnionScratch(const FixedBitmapUnionScratch&) = delete;
    FixedBitmapUnionScratch& operator=(const FixedBitmapUnionScratch&) = delete;
    ~FixedBitmapUnionScratch() { clear(); }

    roaring::Roaring merge(const std::vector<const roaring::Roaring*>& inputs) {
        struct Reset {
            FixedBitmapUnionScratch& scratch;
            ~Reset() { scratch.clear(); }
        } reset{*this};
        for (const auto* bitmap : inputs) {
            const auto& directory = bitmap->roaring.high_low_container;
            for (int32_t i = 0; i < directory.size; ++i) {
                const uint16_t key = directory.keys[i];
                auto& slot = slots_[key];
                uint8_t input_type = directory.typecodes[i];
                const auto* input = roaring::internal::container_unwrap_shared(
                    directory.containers[i], &input_type);
                if (!slot.container) {
                    keys_.push_back(key);
                    slot.type = input_type;
                    slot.container = roaring::internal::container_clone(input, input_type);
                    if (!slot.container) throw std::bad_alloc();
                } else {
                    uint8_t output_type = slot.type;
                    auto* output = roaring::internal::container_lazy_ior(
                        slot.container, slot.type, input, input_type, &output_type);
                    if (!output) throw std::bad_alloc();
                    if (output != slot.container) {
                        roaring::internal::container_free(slot.container, slot.type);
                    }
                    slot.container = output;
                    slot.type = output_type;
                }
            }
        }
        std::sort(keys_.begin(), keys_.end());
        std::unique_ptr<roaring::api::roaring_bitmap_t,
            decltype(&roaring::api::roaring_bitmap_free)> result(
                roaring::api::roaring_bitmap_create_with_capacity(keys_.size()),
                &roaring::api::roaring_bitmap_free);
        if (!result) throw std::bad_alloc();
        for (uint16_t key : keys_) {
            auto& slot = slots_[key];
            slot.container = roaring::internal::container_repair_after_lazy(slot.container, &slot.type);
            if (!slot.container) throw std::bad_alloc();
            roaring::internal::ra_append(&result->high_low_container, key, slot.container, slot.type);
            slot.container = nullptr;
        }
        return roaring::Roaring(result.release());
    }
};
