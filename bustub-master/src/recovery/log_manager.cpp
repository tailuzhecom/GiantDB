//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_manager.h"

namespace bustub {
/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::RunFlushThread() {
    enable_logging = true;
    flush_thread_ = new std::thread([this] {
        while (enable_logging) {
            std::unique_lock<std::mutex> lck(latch_);
            cv_.wait_for(lck, log_timeout);
            std::promise<void> prom;
            int last_lsn = next_lsn_ - 1;
            flush_future_ = prom.get_future().share();
            int flush_size = SwapBuffer();
            lck.unlock();
            disk_manager_->WriteLog(flush_buffer_, flush_size);
            lck.lock();
            SetPersistentLSN(last_lsn);
            prom.set_value();
        }
    });
}

/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
    enable_logging = false;
    std::unique_lock<std::mutex> lck(latch_);
    cv_.notify_one();
    lck.unlock();

    if (flush_thread_ && flush_thread_->joinable())
        flush_thread_->join();
    flush_thread_ = nullptr;
}

void LogManager::WaitForFlushFinish() {
    std::shared_future<void> fut = flush_future_;
    if (fut.valid())
        fut.wait();
    
}

void LogManager::ForceFlush() {
    std::unique_lock<std::mutex> lck(latch_);
    cv_.notify_one();
    std::shared_future<void> fut = flush_future_;
    if (fut.valid())
        fut.wait();
}

int  LogManager::SwapBuffer() {
    int flush_size = offset_;
    std::swap(log_buffer_, flush_buffer_);
    offset_ = 0;
    return flush_size;
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) { 
    std::unique_lock<std::mutex> lck(latch_);

    if (offset_ + log_record->GetSize() >= LOG_BUFFER_SIZE) {
        cv_.notify_one();
        lck.unlock();
        
        std::shared_future<void> fut = flush_future_;
        if (fut.valid())
            fut.wait();
        lck.lock();
    }

    log_record->lsn_ = next_lsn_++;
    mempcpy(log_buffer_ + offset_, log_record, LogRecord::HEADER_SIZE);
    int pos = offset_ + LogRecord::HEADER_SIZE;
    switch (log_record->GetLogRecordType())
    {
    case LogRecordType::INSERT:
        memcpy(log_buffer_ + pos, &log_record->insert_rid_, sizeof(log_record->insert_rid_));
        pos += sizeof(log_record->insert_rid_);
        log_record->insert_tuple_.SerializeTo(log_buffer_ + pos);
        break;
    case LogRecordType::MARKDELETE:
        memcpy(log_buffer_ + pos, &log_record->delete_rid_, sizeof(log_record->delete_rid_));
        pos += sizeof(log_record->delete_rid_);
        log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);
        break;
    case LogRecordType::APPLYDELETE:
        memcpy(log_buffer_ + pos, &log_record->delete_rid_, sizeof(log_record->delete_rid_));
        pos += sizeof(log_record->delete_rid_);
        log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);
        break;
    case LogRecordType::ROLLBACKDELETE:
        memcpy(log_buffer_ + pos, &log_record->delete_rid_, sizeof(log_record->delete_rid_));
        pos += sizeof(log_record->delete_rid_);
        log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);
        break;
    case LogRecordType::UPDATE:
        memcpy(log_buffer_ + pos, &log_record->update_rid_, sizeof(log_record->update_rid_));
        pos += sizeof(log_record->update_rid_);
        log_record->old_tuple_.SerializeTo(log_buffer_ + pos);
        pos += sizeof(int32_t) + log_record->old_tuple_.GetLength();
        log_record->new_tuple_.SerializeTo(log_buffer_ + pos);
        break;
    case LogRecordType::BEGIN:
        break;
    case LogRecordType::COMMIT:
        break;
    case LogRecordType::ABORT:
        break;
    case LogRecordType::NEWPAGE:
        memcpy(log_buffer_ + pos, &log_record->prev_page_id_, sizeof(log_record->prev_page_id_));
        break;
    default:
        break;
    }
    offset_ += log_record->GetSize();
    return log_record->lsn_;

}

}  // namespace bustub
