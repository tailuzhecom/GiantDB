//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_recovery.cpp
//
// Identification: src/recovery/log_recovery.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_recovery.h"

#include "storage/page/table_page.h"

namespace bustub {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data, LogRecord *log_record) { 
    if (offset_ + LogRecord::HEADER_SIZE > LOG_BUFFER_SIZE)
        return false;

    int pos = offset_;
    LogRecord *ptr = (LogRecord*)(data + offset_);
    log_record->size_ = ptr->size_;

    if (ptr->size_ <= 0 || offset_ + ptr->size_ > LOG_BUFFER_SIZE)
        return false;
        
    log_record->lsn_ = ptr->lsn_;
    log_record->txn_id_ = ptr->txn_id_;
    log_record->prev_lsn_ = ptr->prev_lsn_;
    log_record->log_record_type_ = ptr->log_record_type_;
    pos += LogRecord::HEADER_SIZE;

    switch(log_record->log_record_type_) 
    {
    case LogRecordType::INSERT:
        log_record->insert_rid_ = *(RID*)(data + pos);
        pos += sizeof(RID);
        log_record->insert_tuple_.DeserializeFrom(data + pos);
        break;
    case LogRecordType::MARKDELETE:
        log_record->delete_rid_ = *(RID*)(data + pos);
        pos += sizeof(RID);
        log_record->delete_tuple_.DeserializeFrom(data + pos);
        break;
    case LogRecordType::APPLYDELETE:
        log_record->delete_rid_ = *(RID*)(data + pos);
        pos += sizeof(RID);
        log_record->delete_tuple_.DeserializeFrom(data + pos);
        break;
    case LogRecordType::ROLLBACKDELETE:
        log_record->delete_rid_ = *(RID*)(data + pos);
        pos += sizeof(RID);
        log_record->delete_tuple_.DeserializeFrom(data + pos);
        break;
    case LogRecordType::UPDATE:
        log_record->update_rid_ = *(RID*)(data + pos);
        pos += sizeof(RID);
        log_record->old_tuple_.DeserializeFrom(data + pos);
        pos += log_record->old_tuple_.GetLength();
        log_record->new_tuple_.DeserializeFrom(data + pos);
        break;
    case LogRecordType::BEGIN:
        break;
    case LogRecordType::COMMIT:
        break;
    case LogRecordType::ABORT:
        break;
    case LogRecordType::NEWPAGE:
        log_record->prev_page_id_ = *(page_id_t*)(data + pos);
        break;
    default:
        break;
    }
    return true; 
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
    assert(enable_logging == false);
    int log_buffer_offset = 0;
    active_txn_.clear();
    lsn_mapping_.clear();
    int cnt = 0;
    while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, log_buffer_offset)) {
        std::cout << cnt++ << " ReadLog: " << log_buffer_offset << std::endl;
        LogRecord log_record;
        offset_ = log_buffer_offset;
        log_buffer_offset += LOG_BUFFER_SIZE;
        while (DeserializeLogRecord(log_buffer_, &log_record)) {
            std::cout << "execute redo: " << offset_ << std::endl;
            std::cout << log_record.ToString() << std::endl;
            lsn_mapping_[log_record.lsn_] = offset_;
            offset_ += log_record.size_;
            assert(log_record.size_ != 0);
            if (log_record.log_record_type_ == LogRecordType::COMMIT ||
                log_record.log_record_type_ == LogRecordType::ABORT) {
                active_txn_.erase(log_record.txn_id_);
            }
            else {
                active_txn_[log_record.txn_id_] = log_record.lsn_;
            }

            if (log_record.log_record_type_ == LogRecordType::INSERT) {
                page_id_t page_id = log_record.insert_rid_.GetPageId();
                TablePage* page = (TablePage*)buffer_pool_manager_->FetchPage(page_id);
                bool need_to_redo = page->GetLSN() < log_record.lsn_;
                if (need_to_redo)
                    page->InsertTuple(log_record.insert_tuple_, &log_record.insert_rid_, nullptr, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, need_to_redo);
            }
            else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
                page_id_t page_id = log_record.update_rid_.GetPageId();
                TablePage* page = (TablePage*)buffer_pool_manager_->FetchPage(page_id);
                bool need_to_redo = page->GetLSN() < log_record.lsn_;
                if (need_to_redo)
                    page->UpdateTuple(log_record.new_tuple_, &log_record.old_tuple_, log_record.update_rid_, nullptr, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, need_to_redo);
            }
            else if (log_record.log_record_type_ == LogRecordType::MARKDELETE) {
                page_id_t page_id = log_record.delete_rid_.GetPageId();
                TablePage* page = (TablePage*)buffer_pool_manager_->FetchPage(page_id);
                bool need_to_redo = page->GetLSN() < log_record.lsn_;
                if (need_to_redo)
                    page->MarkDelete(log_record.delete_rid_, nullptr, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, need_to_redo);
            }
            else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE) {
                page_id_t page_id = log_record.delete_rid_.GetPageId();
                TablePage* page = (TablePage*)buffer_pool_manager_->FetchPage(page_id);
                bool need_to_redo = page->GetLSN() < log_record.lsn_;
                if (need_to_redo)
                    page->ApplyDelete(log_record.delete_rid_, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, need_to_redo);
            }
            else if (log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
                page_id_t page_id = log_record.delete_rid_.GetPageId();
                TablePage* page = (TablePage*)buffer_pool_manager_->FetchPage(page_id);
                bool need_to_redo = page->GetLSN() < log_record.lsn_;
                if (need_to_redo)
                    page->RollbackDelete(log_record.delete_rid_, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, need_to_redo);
            }
            else if (log_record.log_record_type_ == LogRecordType::NEWPAGE) {
                page_id_t prev_page_id = log_record.prev_page_id_;
                page_id_t new_page_id;
                TablePage* new_page = (TablePage*)buffer_pool_manager_->NewPage(&new_page_id);
                new_page->Init(new_page_id, PAGE_SIZE, prev_page_id, nullptr, nullptr);
                
                if (log_record.prev_page_id_ != INVALID_PAGE_ID) {
                    TablePage* prev_page = (TablePage*)buffer_pool_manager_->FetchPage(prev_page_id);
                    bool need_to_redo = prev_page->GetNextPageId() == INVALID_PAGE_ID;
                    if (need_to_redo)
                        prev_page->SetNextPageId(new_page_id);
                    else  
                        assert(new_page_id == prev_page->GetNextPageId());
                    buffer_pool_manager_->UnpinPage(prev_page_id, need_to_redo);
                }
                buffer_pool_manager_->UnpinPage(new_page_id, true);
            }
        }
    }

}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
    assert(enable_logging == false);
    
    // 遍历未结束的事务，执行undo操作
    for (auto &e : active_txn_) {
        int read_offset = lsn_mapping_[e.second];
        offset_ = 0;
        LogRecord log_record;
        while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, read_offset)) {
            std::cout << "execute undo: " << read_offset << std::endl;
            assert(DeserializeLogRecord(log_buffer_, &log_record)); 
            std::cout << log_record.ToString() << std::endl;
            if (log_record.log_record_type_ == LogRecordType::INSERT) {
                page_id_t page_id = log_record.insert_rid_.GetPageId();
                TablePage* page = (TablePage*)buffer_pool_manager_->FetchPage(page_id);
                bool need_to_redo = page->GetLSN() < log_record.lsn_;
                if (need_to_redo)
                    page->ApplyDelete(log_record.insert_rid_, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, need_to_redo);
            }
            else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
                page_id_t page_id = log_record.update_rid_.GetPageId();
                TablePage* page = (TablePage*)buffer_pool_manager_->FetchPage(page_id);
                bool need_to_redo = page->GetLSN() < log_record.lsn_;
                if (need_to_redo)
                    page->UpdateTuple(log_record.old_tuple_, &log_record.new_tuple_, log_record.update_rid_, nullptr, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, need_to_redo);
            }
            else if (log_record.log_record_type_ == LogRecordType::MARKDELETE) {
                page_id_t page_id = log_record.delete_rid_.GetPageId();
                TablePage* page = (TablePage*)buffer_pool_manager_->FetchPage(page_id);
                bool need_to_redo = page->GetLSN() < log_record.lsn_;
                if (need_to_redo)
                    page->RollbackDelete(log_record.delete_rid_, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, need_to_redo);
            }
            else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE) {
                page_id_t page_id = log_record.delete_rid_.GetPageId();
                TablePage* page = (TablePage*)buffer_pool_manager_->FetchPage(page_id);
                bool need_to_redo = page->GetLSN() < log_record.lsn_;
                if (need_to_redo)
                    page->InsertTuple(log_record.delete_tuple_, &log_record.delete_rid_, nullptr, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, need_to_redo);
            }
            else if (log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
                page_id_t page_id = log_record.delete_rid_.GetPageId();
                TablePage* page = (TablePage*)buffer_pool_manager_->FetchPage(page_id);
                bool need_to_redo = page->GetLSN() < log_record.lsn_;
                if (need_to_redo)
                    page->MarkDelete(log_record.delete_rid_, nullptr, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, need_to_redo);
            }

            if (lsn_mapping_.count(log_record.prev_lsn_) == 0)
                break;
                
            read_offset = lsn_mapping_[log_record.prev_lsn_];
        }
    }

    active_txn_.clear();
    lsn_mapping_.clear();
}

}  // namespace bustub
