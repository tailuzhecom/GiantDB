//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * InsertExecutor executes an insert into a table.
 * Inserted values can either be embedded in the plan itself ("raw insert") or come from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new insert executor.
   * @param exec_ctx the executor context
   * @param plan the insert plan to be executed
   * @param child_executor the child executor to obtain insert values from, can be nullptr
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor)
      : AbstractExecutor(exec_ctx), plan_(plan) {}

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override {
    table_meta_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    table_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get();
  }

  // Note that Insert does not make use of the tuple pointer being passed in.
  // We return false if the insert failed for any reason, and return true if all inserts succeeded.
  bool Next([[maybe_unused]] Tuple *tuple) override {
    std::vector<std::vector<Value>> raw_values = plan_->RawValues();
    RID rid;
    for (unsigned int i = 0; i < raw_values.size(); i++) {
      if (table_->InsertTuple(Tuple(raw_values[i], &table_meta_->schema_), &rid, exec_ctx_->GetTransaction()) == false) 
        return false;
    }
    return true; 
  }

 private:
  /** The insert plan node to be executed. */
  const InsertPlanNode *plan_;
  TableHeap *table_;
  TableMetadata *table_meta_;
};
}  // namespace bustub
