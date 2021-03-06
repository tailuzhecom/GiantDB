//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "common/logger.h"
#include "container/hash/linear_probe_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 20, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, ResizeTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);
  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  // my test
  // insert 120 keys into hash table
  for (int i = 0; i < 1000; i++) {
    EXPECT_EQ(true, ht.Insert(nullptr, i, i));
    std::vector<int> values;
    ht.GetValue(nullptr, i, &values);
    EXPECT_EQ(1, values.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, values[0]);
  }
  // Resize test
  for (int i = 0; i < 1000; i++) {
    std::vector<int> values;
    EXPECT_EQ(true, ht.GetValue(nullptr, i, &values));
    EXPECT_EQ(1, values.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, values[0]);
  }

  // delete all values
  for (int i = 0; i < 1000; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}


std::mutex ht_mtx;
void ConcurrencyThreadFunc(LinearProbeHashTable<int, int, IntComparator> *ht, int n) {

    for (int i = 0; i < 50; i++) {
        int key = i + n;
        std::unique_lock<std::mutex> lck(ht_mtx);
        EXPECT_EQ(true, ht->Insert(nullptr, key, i));
        std::vector<int> values;
        ht->GetValue(nullptr, key, &values);
        EXPECT_EQ(1, values.size()) << "Insert phase Failed to insert " << key << std::endl;
        EXPECT_EQ(i, values[0]);
    }

    for (int i = 0; i < 50; i++) {
        int key = i + n;
        std::unique_lock<std::mutex> lck(ht_mtx);
        std::vector<int> values;
        EXPECT_EQ(true, ht->GetValue(nullptr, key, &values));
        EXPECT_EQ(1, values.size()) << "GetValue phase Failed to insert " << key << std::endl;
        EXPECT_EQ(i, values[0]);
    }
}


TEST(HashTableTest, ConcurrencyTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);
  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  // write multithread code here
  std::thread t1(ConcurrencyThreadFunc, &ht, 0);
  std::thread t2(ConcurrencyThreadFunc, &ht, 105);
  std::thread t3(ConcurrencyThreadFunc, &ht, 300);
  t1.join();
  t2.join();
  t3.join();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
