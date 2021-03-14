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
#include "buffer/clock_replacer.h"

#include <list>
#include <unordered_map>

using namespace std;

namespace bustub {

//-----------------------------------------------------------------------------

BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     DiskManager *disk_manager,
                                     LogManager *log_manager) :
  pool_size_(pool_size),
  disk_manager_(disk_manager),
  log_manager_(log_manager) {

  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

//-----------------------------------------------------------------------------

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

//-----------------------------------------------------------------------------

Page *BufferPoolManager::FetchPageImpl(const page_id_t page_id) {
  unique_lock<mutex> scoped_lock(latch_);

  // 1.     Search the page table for the requested page (P).
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    // 1.1    If P exists, pin it and return it immediately.
    Page *const page = &pages_[iter->second];
    page->WLatch();
    ++page->pin_count_;
    page->WUnlatch();

    replacer_->Pin(iter->second);
    return page;
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the
  //        free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t frame_id = GetNextAvailableFrame();
  if (frame_id == -1) {
    // All frames are being occupied by pinned pages. Not enough memory to
    // bring a new page in.
    return nullptr;
  }

  // 2.     If R is dirty, write it back to the disk.
  // Flush the page whose frame we are taking if necessary.
  MaybeEvictPageFromFrame(frame_id);

  // 3.2 Insert P into the page table.
  page_table_[page_id] = frame_id;

  // 4.     Update P's metadata, read in the page content from disk, and then
  //        return a pointer to P.
  Page *const page = &pages_[frame_id];
  ResetPage(page, page_id, 1 /* new_pin_count */);

  // Read in contents from disk.
  page->WLatch();
  disk_manager_->ReadPage(page_id, page->GetData());
  page->WUnlatch();

  // Remove from replacer if needed.
  replacer_->Pin(frame_id);

  return page;
}

//-----------------------------------------------------------------------------

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, const bool is_dirty) {
  unique_lock<mutex> scoped_lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    // No page to decrement pinning.
    return true;
  }

  Page *const page = &pages_[iter->second];

  page->RLatch();
  int pin_count = page->GetPinCount();
  page->RUnlatch();
  if (pin_count <= 0) {
    return false;
  }

  page->WLatch();
  // Decrement pin count.
  --page->pin_count_;
  pin_count = page->GetPinCount();

  // Set the page as dirty if needed.
  page->is_dirty_ = page->is_dirty_ || is_dirty;
  page->WUnlatch();

  if (pin_count == 0) {
    // We have decremented to no pins. Add the page for replacement.
    replacer_->Unpin(iter->second);
  }
  return true;
}

//-----------------------------------------------------------------------------

bool BufferPoolManager::FlushPageImpl(const page_id_t page_id) {
  unique_lock<mutex> scoped_lock(latch_);
  return FlushPageLocked(page_id);
}

//-----------------------------------------------------------------------------

bool BufferPoolManager::FlushPageLocked(const page_id_t page_id) {
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  Page *const page = &pages_[iter->second];
  page->RLatch();
  const bool is_dirty = page->IsDirty();
  page->RUnlatch();

  if (!is_dirty) {
    // This page is untouched. No need to write to disk.
    return true;
  }

  page->WLatch();
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  page->WUnlatch();
  return true;
}

//-----------------------------------------------------------------------------

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  unique_lock<mutex> scoped_lock(latch_);
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer.
  //      Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = disk_manager_->AllocatePage();
  frame_id_t frame_id = GetNextAvailableFrame();

  if (frame_id == -1) {
    return nullptr;
  }

  // Flush and evict the existing page.
  MaybeEvictPageFromFrame(frame_id);

  Page *const page = &pages_[frame_id];
  ResetPage(page, *page_id, 1 /* new_pin_count */);

  // Update the page table.
  page_table_[*page_id] = frame_id;
  return page;
}

//-----------------------------------------------------------------------------

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  unique_lock<mutex> scoped_lock(latch_);

  // 0.   Make sure you call DiskManager::DeallocatePage!
  disk_manager_->DeallocatePage(page_id);

  // 1.   Search the page table for the requested page (P).
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    // 1.   If P does not exist, return true.
    return true;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is
  //      using the page.
  Page *const page = &pages_[iter->second];
  page->RLatch();
  bool is_pinned = page->GetPinCount();
  page->RUnlatch();
  if (is_pinned) {
    return false;
  }

  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its
  //      metadata and return it to the free list.
  // Add frame to free list.
  free_list_.push_back(iter->second);

  // Delete from page table and reset page.
  page_table_.erase(iter);
  ResetPage(page, INVALID_PAGE_ID);
  return false;
}

//-----------------------------------------------------------------------------

void BufferPoolManager::FlushAllPagesImpl() {
  unique_lock<mutex> scoped_lock(latch_);

  for (const pair<const page_id_t, frame_id_t>& page_frame : page_table_) {
    FlushPageLocked(page_frame.first);
  }
}

//-----------------------------------------------------------------------------

frame_id_t BufferPoolManager::GetNextAvailableFrame() {
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&frame_id);
  }
  return frame_id;
}

//-----------------------------------------------------------------------------

bool BufferPoolManager::MaybeEvictPageFromFrame(const frame_id_t frame_id) {
  assert(frame_id < static_cast<int>(pool_size_));
  assert(frame_id >= 0);

  if (pages_[frame_id].page_id_ != INVALID_PAGE_ID) {
    FlushPageLocked(pages_[frame_id].page_id_);
    // 3.1     Delete R from the page table.
    page_table_.erase(pages_[frame_id].page_id_);
    return true;
  }
  return false;
}

//-----------------------------------------------------------------------------

void BufferPoolManager::ResetPage(Page *const page,
                                  const page_id_t new_page_id,
                                  const int new_pin_count) {
  assert(page);

  // Clear page contents.
  page->WLatch();
  page->ResetMemory();
  page->page_id_ = new_page_id;
  page->pin_count_ = new_pin_count;
  page->is_dirty_ = false;
  page->WUnlatch();
}

//-----------------------------------------------------------------------------

}  // namespace bustub
