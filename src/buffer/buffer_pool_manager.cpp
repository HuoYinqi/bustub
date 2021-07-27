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
  replacer_ = new LRUReplacer(pool_size);

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
  frame_id_t frame_id;
  Page *page;
  std::lock_guard<std::mutex> guard_lock(latch_);
  if (isInPageTable(page_id))
  {
    frame_id = page_table_[page_id];
    page = pages_ + frame_id;
    replacer_->Pin(frame_id);
    page->pin_count_ ++;
    return page;
  }
  else{
    if (!free_list_.empty())
    {
      frame_id = free_list_.front();
      free_list_.pop_front();
      page = pages_ + frame_id;
      disk_manager_->ReadPage(page_id, page->data_);
      page->page_id_ = page_id;
      page->pin_count_ ++;
      page_table_[page_id] = frame_id;
      replacer_->Pin(frame_id);
      return page;
    }
    else{
      frame_id_t vect_id;
      if (!(replacer_->Victim(&vect_id))) return nullptr;
      page = pages_ + vect_id;

      if (pages_[vect_id].IsDirty())
      {
        disk_manager_->WritePage(page->page_id_, page->data_);
      }
      page_table_.erase(page_table_.find(page->page_id_));
      page->ResetMemory();
      page->page_id_ = page_id;
      disk_manager_->ReadPage(page_id, page->data_);
      page->pin_count_ ++;
      page_table_[page_id] = vect_id;
      replacer_->Pin(vect_id);
      return page;
    }
  }
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty)
{
  std::lock_guard<std::mutex> guard_lock(latch_);
  if (!isInPageTable(page_id)) return false;

  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;
  if (page->pin_count_ <= 0) return false;

  page->is_dirty_ = is_dirty;
  page->pin_count_ --;
  replacer_->Unpin(frame_id);
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> guard_lock(latch_);
  if (!isInPageTable(page_id)) return false;

  frame_id_t frame_id = page_table_[page_id];
  Page * page = pages_ + frame_id;
  disk_manager_->WritePage(page->page_id_, page->data_);
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guard_lock(latch_);
  if (free_list_.empty() && (replacer_->Size() == 0)) return nullptr;
  Page *new_page;
  *page_id = disk_manager_->AllocatePage();
  frame_id_t frame_id;
  if (free_list_.empty())
  {
    replacer_->Victim(&frame_id);
    if (pages_[frame_id].IsDirty())
    {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
    page_table_.erase(page_table_.find(pages_[frame_id].page_id_));
    pages_[frame_id].ResetMemory();
    new_page = pages_ + frame_id;
    replacer_->Pin(frame_id);
    new_page ->pin_count_ ++;
    new_page ->page_id_ = *page_id;
    page_table_[*page_id] = frame_id;
  }
  else{
    frame_id = free_list_.front();
    free_list_.pop_front();
    new_page = pages_ + frame_id;
    replacer_->Pin(frame_id);
    new_page ->page_id_ = *page_id;
    new_page -> pin_count_ ++;
    page_table_[new_page->page_id_] = frame_id;
  }

  return new_page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard_lock(latch_);
  if (!isInPageTable(page_id)) return true;
  
  frame_id_t frame_id = page_table_[page_id];
  Page *page  = pages_ + frame_id;
  if (page->pin_count_ > 0) return false;
  
  replacer_->Pin(frame_id);
  disk_manager_->DeallocatePage(page->page_id_);
  page_table_.erase(page_table_.find(page->page_id_));
  page->ResetMemory();
  free_list_.push_back(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::lock_guard<std::mutex> guard_lock(latch_);
  for (int i = 0; i < (int) pool_size_; i++)
  {
    if (pages_[i].page_id_ != INVALID_PAGE_ID && pages_[i].IsDirty())
    {
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
    } 
  }


  // You can do it!
}

}  // namespace bustub
