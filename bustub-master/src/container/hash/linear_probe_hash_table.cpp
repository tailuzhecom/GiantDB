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
        size_ = slot_num_per_page_;
        Resize(num_buckets);
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
    table_latch_.RLock();
    int64_t hash_val = hash_fn_.GetHash(key) % size_;   // get hash value
    page_id_t bucket_id = hash_val / slot_num_per_page_;  // get page id
    int slot_idx = hash_val % slot_num_per_page_;   // get slot_idx'th slot in the page
    // get header page
    auto header_page = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
    auto bucket_page_id = header_page->GetBlockPageId(bucket_id);
    // get bucket page
    auto bucket_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
    bool ret = false;
    // get the slot to insert
    while (bucket_page->IsOccupied(slot_idx)) {
        if (bucket_page->IsReadable(slot_idx) && comparator_(bucket_page->KeyAt(slot_idx), key) == 0) {
            result->push_back(bucket_page->ValueAt(slot_idx));
            ret = true;
        }

        slot_idx++;
        hash_val++;

        if (hash_val >= (int)size_)
            break;

        // to the end of current page, get next page
        if (slot_idx >= slot_num_per_page_) {
            buffer_pool_manager_->UnpinPage(bucket_page_id, false);
            bucket_id++;
            bucket_page_id = header_page->GetBlockPageId(bucket_id);
            bucket_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
            slot_idx = 0;
        }
    }

    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.RUnlock();
    return ret;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
    if (resize_thread_id_ != std::this_thread::get_id() || is_resizing_ == false) {
        table_latch_.WLock();
    }
    is_inserting_ = true;
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
        hash_val++;
        // resize
        if (hash_val >= (int)size_) {
            Resize(size_ * 2);
        }

        // to the end of current page, get next page
        if (slot_idx >= slot_num_per_page_) {
            buffer_pool_manager_->UnpinPage(bucket_page_id, false);
            bucket_id++;  //　换页
            // current page is the last page in this hash table, return false
            if (bucket_id >= 4080) {
                LOG_INFO("Insert failed\n");
                buffer_pool_manager_->UnpinPage(header_page_id_, header_page_modified);
                if (resize_thread_id_ != std::this_thread::get_id() || is_resizing_ == false)
                    table_latch_.WUnlock();
                is_inserting_ = false;
                return false;
            }
            bucket_page_id = header_page->GetBlockPageId(bucket_id);
            bucket_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
            slot_idx = 0;
        }
    }

    if (is_duplicated) {
        buffer_pool_manager_->UnpinPage(bucket_id, false);
        buffer_pool_manager_->UnpinPage(header_page_id_, header_page_modified);
        if (resize_thread_id_ != std::this_thread::get_id() || is_resizing_ == false)
            table_latch_.WUnlock();
        is_inserting_ = false;
        return false;
    }
    else {
        bool ret = false;
        ret = bucket_page->Insert(slot_idx, key, value);
        buffer_pool_manager_->UnpinPage(bucket_id, true);
        buffer_pool_manager_->UnpinPage(header_page_id_, header_page_modified);
        if (resize_thread_id_ != std::this_thread::get_id() || is_resizing_ == false)
            table_latch_.WUnlock();
        is_inserting_ = false;
        return ret;
    }

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
    if (resize_thread_id_ != std::this_thread::get_id() || is_resizing_ == false)
        table_latch_.WLock();
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
        hash_val++;

        if (hash_val >= (int)size_)
            break;

        // to the end of current page, get next page
        if (slot_idx >= slot_num_per_page_) {
            buffer_pool_manager_->UnpinPage(bucket_page_id, false);
            bucket_id++;
            // current page is the last page in this hash table, return false
            bucket_page_id = header_page->GetBlockPageId(bucket_id);
            bucket_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
            slot_idx = 0;
        }
    }
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    if (resize_thread_id_ != std::this_thread::get_id() || is_resizing_ == false)
        table_latch_.WUnlock();
    return ret;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
    if (is_inserting_ == false)
        table_latch_.WLock();
    resize_thread_id_ =  std::this_thread::get_id();
    is_resizing_ = true;
    int new_page_num = initial_size / slot_num_per_page_;  //　一共需要new_page_num个page
    if (initial_size % slot_num_per_page_ != 0)
        new_page_num++;

    int new_page_id;
    auto header_page = reinterpret_cast<HashTableHeaderPage*>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
    int i;
    // LOG_INFO("new pages\n");
    for (i = header_page->GetSize(); i < (int)new_page_num; i++) {
        if (buffer_pool_manager_->NewPage(&new_page_id)) {
            header_page->AddBlockPageId(new_page_id);
        }
        else{
            // page用尽
            LOG_INFO("page out of usage\n");
            buffer_pool_manager_->UnpinPage(header_page_id_, true);
            if (is_inserting_ == false)
                table_latch_.WUnlock();
            return;
        }
    }

    // LOG_INFO("rehash\n");
    int old_size = size_;
    size_ = initial_size; // 更新size
    for (i = 0; i < (int)old_size; i++) {
        int old_page_idx = i / slot_num_per_page_;  // 对应page在header page中的idx
        int old_slot_idx = i % slot_num_per_page_;
        int old_page_id =  header_page->GetBlockPageId(old_page_idx); // 对应page的page_id_t
        // 对应的block_page
        auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(buffer_pool_manager_->FetchPage(old_page_id));

        if (block_page->IsReadable(old_slot_idx)) {
            block_page->Remove(old_slot_idx);  // 先删除再插入
            auto key = block_page->KeyAt(old_slot_idx);
            auto val = block_page->ValueAt(old_slot_idx);
            Insert(nullptr, key, val);
        }

        buffer_pool_manager_->UnpinPage(header_page->GetBlockPageId(old_page_id), true);
    }

    buffer_pool_manager_->UnpinPage(header_page_id_, true);
    size_ = initial_size;
    is_resizing_ = false;
    if (is_inserting_ == false)
        table_latch_.WUnlock();
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
