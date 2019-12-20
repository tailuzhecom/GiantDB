//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// rwmutex.h
//
// Identification: src/include/common/rwlatch.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <climits>
#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT

#include "common/macros.h"

namespace bustub {

/**
 * Reader-Writer latch backed by std::mutex.
 */
class ReaderWriterLatch {
  using mutex_t = std::mutex;
  using cond_t = std::condition_variable;
  static const uint32_t MAX_READERS = UINT_MAX;

 public:
  ReaderWriterLatch() = default;
  ~ReaderWriterLatch() { std::lock_guard<mutex_t> guard(mutex_); }

  DISALLOW_COPY(ReaderWriterLatch);

  /**
   * Acquire a write latch.
   */
  void WLock() {
    std::unique_lock<mutex_t> latch(mutex_);
    // 等待另外一个写进程结束
    while (writer_entered_) {
      reader_.wait(latch);
    }
    writer_entered_ = true;
    // 等待所有的读进程结束
    while (reader_count_ > 0) {
      writer_.wait(latch);
    }
  }

  /**
   * Release a write latch.
   */
  void WUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
    writer_entered_ = false;
    // 通知所有的读进程
    reader_.notify_all();
  }

  /**
   * Acquire a read latch.
   */
  void RLock() {
    std::unique_lock<mutex_t> latch(mutex_);
    // 如果有写进程或者读进程数量达到上限，进入等待
    while (writer_entered_ || reader_count_ == MAX_READERS) {
      reader_.wait(latch);
    }
    reader_count_++;
  }

  /**
   * Release a read latch.
   */
  void RUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
    reader_count_--;
    // 如果有写进程在等待，那么在读进程全部结束后唤醒写进程
    if (writer_entered_) {
      if (reader_count_ == 0) {
        writer_.notify_one();
      }
    } else {
      if (reader_count_ == MAX_READERS - 1) { // 用于处理读进程达到上限的情况
        reader_.notify_one();
      }
    }
  }

 private:
  mutex_t mutex_;
  cond_t writer_;
  cond_t reader_;
  uint32_t reader_count_{0};
  bool writer_entered_{false};
};

}  // namespace bustub
