//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);

  // 如果页表中存在该页
  if (page_table_.count(page_id)) {
      return pages_ + page_table_[page_id];
  }
  else {
      frame_id_t frame = -1;
      // 查看free_list是否有可以用页
      if (!free_list_.empty()) {
          frame = free_list_.front();
          free_list_.pop_front();
      }
      else {
          // replacer中没有淘汰页
          if (!replacer_->Victim(&frame))
              return nullptr;
      }

      page_id_t old_page_id = -1;
      for (auto iter = page_table_.begin(); iter != page_table_.end(); ++iter) {
          if (iter->second == frame) {
              old_page_id = iter->first;
              page_table_.erase(iter);
              page_table_[page_id] = frame;
              break;
          }
      }
      // 将脏页写回硬盘
      if (pages_[frame].is_dirty_) {
          disk_manager_->WritePage(old_page_id, pages_[frame].data_);
      }

      // 更新页面信息
      pages_[frame].is_dirty_ = false;
      pages_[frame].page_id_ = page_id;
      pages_[frame].pin_count_ = 1;
      // 从硬盘读取信息到内存页
      disk_manager_->ReadPage(page_id, pages_[frame].data_);
  }

  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
    std::lock_guard<std::mutex> lock(latch_);

    frame_id_t frame_id = page_table_[page_id];
    if (pages_[frame_id].pin_count_ <= 0)
        return false;

    pages_[frame_id].is_dirty_ = is_dirty;
    pages_[frame_id].pin_count_--;

    // 如果pin count为0,将该frame放到replacer
    if (pages_[frame_id].pin_count_ == 0) {
        replacer_->Unpin(frame_id);
    }
    return true;
}

// 将page_id对应的页面写回硬盘
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (page_id == INVALID_PAGE_ID)
      return false;

  if (page_table_.find(page_id) != page_table_.end()) {
      frame_id_t frame_id = page_table_[page_id];
      if (pages_[frame_id].is_dirty_) {
          disk_manager_->WritePage(page_id, pages_[frame_id].data_);
          pages_[frame_id].is_dirty_ = false;
      }
  }
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);

  *page_id = disk_manager_->AllocatePage();
  frame_id_t frame = -1;

  // 查看free_list是否有可以用页
  if (!free_list_.empty()) {
      frame = free_list_.front();
      free_list_.pop_front();
  }
  else {
      // replacer中没有淘汰页
      if (!replacer_->Victim(&frame))
          return nullptr;
  }


  page_id_t old_page_id = -1;
  for (auto iter = page_table_.begin(); iter != page_table_.end(); ++iter) {
      if (iter->second == frame) {
            old_page_id = iter->first;
            page_table_.erase(iter);
            page_table_[*page_id] = frame;
            break;
        }
  }
  if (pages_[frame].is_dirty_) {
      disk_manager_->WritePage(old_page_id, pages_[frame].data_);
  }

  // 更新页面信息
  pages_[frame].is_dirty_ = false;
  pages_[frame].page_id_ = *page_id;
  pages_[frame].pin_count_ = 1;
  pages_[frame].ResetMemory();

  return pages_ + frame;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.find(page_id) == page_table_.end())
      return true;

  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_)
      return false;
  else {
      page_table_.erase(page_id);
      pages_[frame_id].ResetMemory();
      free_list_.push_back(frame_id);
      return true;
  }
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard<std::mutex> lock(latch_);
  for (const auto &e : page_table_) {
      if (pages_[e.second].is_dirty_) {
          disk_manager_->WritePage(e.first, pages_[e.second].data_);
          pages_[e.second].is_dirty_ = false;
      }
  }
}

}  // namespace bustub
