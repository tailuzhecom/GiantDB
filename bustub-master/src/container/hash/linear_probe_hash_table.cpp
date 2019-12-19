//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {
// TODO concurrency control
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
        auto header_page_ = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->NewPage(&header_page_id_));
        page_id_t hash_table_first_bucket;
        auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(buffer_pool_manager->NewPage(&hash_table_first_bucket));
        slot_num_per_page_ = block_page->SlotNum();
        header_page_->AddBlockPageId(hash_table_first_bucket);
        size_ = 1;
        Resize(num_buckets);
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
    int64_t hash_val = hash_fn_.GetHash(key) % size_;   // get hash value
    page_id_t bucket_id = hash_val / slot_num_per_page_;  // get page id
    int slot_idx = hash_val % slot_num_per_page_;   // get slot_idx'th slot in the page
    // get header page
    auto header_page = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
    auto bucket_page_id = header_page->GetBlockPageId(bucket_id);
    // get bucket page
    auto bucket_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());

    // get the slot to insert
    while (bucket_page->IsOccupied(slot_idx)) {
        if (bucket_page->IsReadable(slot_idx) && comparator_(bucket_page->KeyAt(slot_idx), key) == 0) {
            result->push_back(bucket_page->ValueAt(slot_idx));
        }

        slot_idx++;
        // to the end of current page, get next page
        if (slot_idx >= slot_num_per_page_) {
            buffer_pool_manager_->UnpinPage(bucket_page_id, false);
            bucket_id++;
            // current page is the last page in this hash table, return false
            if ((size_t )bucket_id >= header_page->GetSize()) {
                buffer_pool_manager_->UnpinPage(header_page_id_, false);
                return true;
            }
            bucket_page_id = header_page->GetBlockPageId(bucket_id);

            bucket_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
            slot_idx = 0;
        }
    }

    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    return true;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
    int64_t hash_val = hash_fn_.GetHash(key) % size_;   // get hash value
    page_id_t bucket_id = hash_val / slot_num_per_page_;  // get page id
    int slot_idx = hash_val % slot_num_per_page_;   // get slot_idx'th slot in the page

  // get header page
  auto header_page = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
  auto bucket_page_id = header_page->GetBlockPageId(bucket_id);
  // get bucket page
  auto bucket_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());

  bool is_duplicated = false;
  bool header_page_modified = false;
  // get the slot to insert
  while (bucket_page->IsReadable(slot_idx)) {
      // not allow duplicated item in hash table
      if (comparator_(bucket_page->KeyAt(slot_idx), key) == 0 && bucket_page->ValueAt(slot_idx) == value) {
          is_duplicated = true;
          break;
      }

      slot_idx++;
      // to the end of current page, get next page
      if (slot_idx >= slot_num_per_page_ || (bucket_id * slot_num_per_page_ + slot_idx >= (int)size_)) {
          buffer_pool_manager_->UnpinPage(bucket_page_id, false);
          bucket_id++;
          // current page is the last page in this hash table, return false
          if ((size_t )bucket_id >= header_page->GetSize()) {
              if (bucket_id >= 4080) {
                  buffer_pool_manager_->UnpinPage(header_page_id_, header_page_modified);
                  return false;
              }
              else {
                  Resize(size_ * 2);
              }
          }
          bucket_page_id = header_page->GetBlockPageId(bucket_id);
          bucket_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
          slot_idx = 0;
      }
  }

  if (is_duplicated) {
      buffer_pool_manager_->UnpinPage(bucket_id, false);
      buffer_pool_manager_->UnpinPage(header_page_id_, header_page_modified);
      return false;
  }
  else {
      bool ret = false;
      ret = bucket_page->Insert(slot_idx, key, value);
      buffer_pool_manager_->UnpinPage(bucket_id, true);
      buffer_pool_manager_->UnpinPage(header_page_id_, header_page_modified);
      return ret;
  }

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
    int64_t hash_val = hash_fn_.GetHash(key) % size_;   // get hash value
    page_id_t bucket_id = hash_val / slot_num_per_page_;  // get page id
    int slot_idx = hash_val % slot_num_per_page_;   // get slot_idx'th slot in the page

    // get header page
    auto header_page = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
    auto bucket_page_id = header_page->GetBlockPageId(bucket_id);
    // get bucket page
    auto bucket_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
    bool ret = false;
    while (bucket_page->IsOccupied(slot_idx)) {
        if (bucket_page->IsReadable(slot_idx) && comparator_(bucket_page->KeyAt(slot_idx), key) == 0 && bucket_page->ValueAt(slot_idx) == value) {
            bucket_page->Remove(slot_idx);
            ret = true;
            break;
        }

        slot_idx++;
        // to the end of current page, get next page
        if (slot_idx >= slot_num_per_page_) {
            buffer_pool_manager_->UnpinPage(bucket_page_id, false);
            bucket_id++;
            // current page is the last page in this hash table, return false
            if ((size_t )bucket_id >= header_page->GetSize()) {
                buffer_pool_manager_->UnpinPage(header_page_id_, false);
                return true;
            }

            bucket_page_id = header_page->GetBlockPageId(bucket_id);
            bucket_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
            slot_idx = 0;
        }
    }
    buffer_pool_manager_->UnpinPage(bucket_id, false);
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    return ret;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
    int new_page_num = initial_size / slot_num_per_page_;
    if (initial_size % slot_num_per_page_ != 0)
        new_page_num++;

    int new_page_id;
    auto header_page = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
    int i;
    for (i = 0; i < (int)new_page_num; i++) {
        if (buffer_pool_manager_->NewPage(&new_page_id)) {
            header_page->AddBlockPageId(new_page_id);
        }
        else{
            LOG_INFO("page out of usage\n");
            break;
        }
    }
    buffer_pool_manager_->UnpinPage(header_page_id_, true);
    size_ = initial_size;
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return size_;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
