//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "container/hash/linear_probe_hash_table.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * IdentityHashFunction hashes everything to itself, i.e. h(x) = x.
 */
class IdentityHashFunction : public HashFunction<hash_t> {
 public:
  /**
   * Hashes the key.
   * @param key the key to be hashed
   * @return the hashed value
   */
  uint64_t GetHash(size_t key) override { return key; }
};

/**
 * A simple hash table that supports hash joins.
 */
class SimpleHashJoinHashTable {
 public:
  /** Creates a new simple hash join hash table. */
  SimpleHashJoinHashTable(const std::string &name, BufferPoolManager *bpm, HashComparator cmp, uint32_t buckets,
                          const IdentityHashFunction &hash_fn) {}

  /**
   * Inserts a (hash key, tuple) pair into the hash table.
   * @param txn the transaction that we execute in
   * @param h the hash key
   * @param t the tuple to associate with the key
   * @return true if the insert succeeded
   */
  bool Insert(Transaction *txn, hash_t h, const Tuple &t) {
    hash_table_[h].emplace_back(t);
    return true;
  }

  /**
   * Gets the values in the hash table that match the given hash key.
   * @param txn the transaction that we execute in
   * @param h the hash key
   * @param[out] t the list of tuples that matched the key
   */
  void GetValue(Transaction *txn, hash_t h, std::vector<Tuple> *t) { *t = hash_table_[h]; }

 private:
  std::unordered_map<hash_t, std::vector<Tuple>> hash_table_;
};

// TODO(student): when you are ready to attempt task 3, replace the using declaration!
using HT = SimpleHashJoinHashTable;

// using HashJoinKeyType = ???;
// using HashJoinValType = ???;
// using HT = LinearProbeHashTable<HashJoinKeyType, HashJoinValType, HashComparator>;

/**
 * HashJoinExecutor executes hash join operations.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new hash join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the hash join plan node
   * @param left the left child, used by convention to build the hash table
   * @param right the right child, used by convention to probe the hash table
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan, std::unique_ptr<AbstractExecutor> &&left,
                   std::unique_ptr<AbstractExecutor> &&right)
      : AbstractExecutor(exec_ctx), 
        plan_(plan),
        left_executor_(std::move(left)),
        right_executor_(std::move(right))  {}

  /** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
  // Uncomment me! const HT *GetJHT() const { return &jht_; }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override {
    left_executor_->Init();
    right_executor_->Init();
    Tuple cur_tuple;
    left_idx = 0;
    right_idx = 0;

    while (left_executor_->Next(&cur_tuple)) {
      left_tuples.push_back(cur_tuple);
    }

    while (right_executor_->Next(&cur_tuple)) {
      right_tuples.push_back(cur_tuple);
    }

    left_schema = left_executor_->GetOutputSchema();
    right_schema = right_executor_->GetOutputSchema();
    output_schema = plan_->OutputSchema();
    std::vector<Column> output_cols = output_schema->GetColumns();
    std::vector<Column> left_cols = left_schema->GetColumns();
    std::vector<Column> right_cols = right_schema->GetColumns();

    for (unsigned int i = 0; i < output_cols.size(); i++) {
    for (unsigned int j = 0; j < left_cols.size(); j++) {
      if (output_cols[i].GetName() == left_cols[j].GetName()) {
        output_order.push_back({0, j});
      }
    }

    for (unsigned int j = 0; j < right_cols.size(); j++) {
      if (output_cols[i].GetName() == right_cols[j].GetName()) {
        output_order.push_back({1, j});
      }
    }
    }

    left_tuples_size = left_tuples.size();
    right_tuples_size = right_tuples.size();
    total_size = left_tuples_size * right_tuples_size;
    idx = 0;
  }

  bool Next(Tuple *tuple) override { 
    Tuple cur_tuple;

    for (; idx < total_size; idx++) {
        int left_idx = idx / right_tuples_size;
        int right_idx = idx % right_tuples_size;
        if (plan_->Predicate()->EvaluateJoin(&left_tuples[left_idx], left_schema, &right_tuples[right_idx], right_schema).GetAs<bool>()) {
          std::vector<Value> values;

          for (const auto &order : output_order) {
            if (order.first == 1)
              values.push_back(left_tuples[left_idx].GetValue(left_schema, order.second));
            else 
              values.push_back(right_tuples[right_idx].GetValue(right_schema, order.second));
          }
          *tuple = Tuple(values, output_schema);
          idx++;
          
          return true;
      }
    }
    return false;
  }

  /**
   * Hashes a tuple by evaluating it against every expression on the given schema, combining all non-null hashes.
   * @param tuple tuple to be hashed
   * @param schema schema to evaluate the tuple on
   * @param exprs expressions to evaluate the tuple with
   * @return the hashed tuple
   */
  hash_t HashValues(const Tuple *tuple, const Schema *schema, const std::vector<const AbstractExpression *> &exprs) {
    hash_t curr_hash = 0;
    // For every expression,
    for (const auto &expr : exprs) {
      // We evaluate the tuple on the expression and schema.
      Value val = expr->Evaluate(tuple, schema);
      // If this produces a value,
      if (!val.IsNull()) {
        // We combine the hash of that value into our current hash.
        curr_hash = HashUtil::CombineHashes(curr_hash, HashUtil::HashValue(&val));
      }
    }
    return curr_hash;
  }

 private:
  /** The hash join plan node. */
  const HashJoinPlanNode *plan_;
  /** The comparator is used to compare hashes. */
  [[maybe_unused]] HashComparator jht_comp_{};
  /** The identity hash function. */
  IdentityHashFunction jht_hash_fn_{};

  /** The hash table that we are using. */
  // Uncomment me! HT jht_;
  /** The number of buckets in the hash table. */
  static constexpr uint32_t jht_num_buckets_ = 2;

  std::vector<Tuple> left_tuples, right_tuples;
  std::vector<std::pair<int, int> > output_order;
  int left_tuples_size;
  int right_tuples_size;
  int total_size;
  int left_idx;
  int right_idx;
  int idx;
  const Schema *left_schema;
  const Schema *right_schema;
  const Schema *output_schema;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  
};
}  // namespace bustub
