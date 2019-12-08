//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    cur_ptr_ = 0;
    num_pages_ = num_pages;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
    if (Size() == 0)
        return false;

    while (true) {
        if (clock_set_[cur_ptr_].second == 0) {
            *frame_id = clock_set_[cur_ptr_].first;
            clock_set_.erase((clock_set_.begin() + cur_ptr_));

            return true;
        }
        else
            clock_set_[cur_ptr_].second = 0;
        cur_ptr_ = (cur_ptr_ + 1) % clock_set_.size();
    }
    return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    for (unsigned long i = 0; i < clock_set_.size(); i++) {
        if (clock_set_[i].first == frame_id) {
            clock_set_.erase(clock_set_.begin() + i);
            cur_ptr_ = i;
            return;
        }
    }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    for (unsigned long i = 0; i < clock_set_.size(); i++) {
        if (clock_set_[i].first == frame_id) {
            clock_set_[i].second = 1;
            return;
        }
    }
    clock_set_.push_back({frame_id, 1});
}

size_t ClockReplacer::Size() {
    return clock_set_.size();
}

}  // namespace bustub
