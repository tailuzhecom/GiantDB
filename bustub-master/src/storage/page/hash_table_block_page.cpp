//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <include/common/logger.h>
#include "storage/page/hash_table_block_page.h"
#include "storage/index/generic_key.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
  return array_[bucket_ind].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  return array_[bucket_ind].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
//    LOG_INFO("block page readable size: %ld\n", sizeof(readable_));
//    LOG_INFO("block page occupied size: %ld\n", sizeof(occupied_));
//    LOG_INFO("block page array size: %ld\n", sizeof(array_));

    int byte_num = bucket_ind / 8;
    int bit_num = bucket_ind % 8;
    unsigned char and_val = (1 << bit_num);

    if (readable_[byte_num] & and_val)
        return false;

    array_[bucket_ind].first = key;
    array_[bucket_ind].second = value;
    occupied_[byte_num] |= and_val;
    readable_[byte_num] |= and_val;
    return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
    int byte_num = bucket_ind / 8;
    int bit_num = bucket_ind % 8;
    readable_[byte_num] &= ~(1 << bit_num);  // TODO
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
    int byte_num = bucket_ind / 8;
    int bit_num = bucket_ind % 8;
    return occupied_[byte_num] & (1 << bit_num);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
  int byte_num = bucket_ind / 8;
  int bit_num = bucket_ind % 8;
  return readable_[byte_num] & (1 << bit_num);
}

// return the slot num in this page
template<typename KeyType, typename ValueType, typename KeyComparator>
int HASH_TABLE_BLOCK_TYPE::SlotNum() {
//    int slot_num = (PAGE_SIZE - sizeof(readable_) - sizeof(occupied_)) / sizeof(MappingType);
//    LOG_INFO("slot num in block page: %d\n", slot_num);
    int slot_num = sizeof(readable_) * 8;
    LOG_INFO("slot num: %d\n", slot_num);
    return slot_num;
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
