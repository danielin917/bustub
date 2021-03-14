//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <tuple>

#include "buffer/clock_replacer.h"

using namespace std;

namespace bustub {

//-----------------------------------------------------------------------------

ClockReplacer::ClockReplacer(const size_t num_pages) :
  clock_hand_(frame_list_.end()) {
}

//-----------------------------------------------------------------------------

ClockReplacer::~ClockReplacer() = default;

//-----------------------------------------------------------------------------

bool ClockReplacer::Victim(frame_id_t *const frame_id) {
  unique_lock scoped_lock(latch_);
  assert(frame_id);
  if (clock_hand_ == frame_list_.end()) {
    return false;
  }

  while (clock_hand_->reference_bit) {
    clock_hand_->reference_bit = false;
    NextClockHand();
  }
  *frame_id = clock_hand_->frame_id;

  // Remove the clockhand entry. This will also take care of moving the clock
  // hand forward.
  RemoveFrame(*frame_id);
  return true;
}

//-----------------------------------------------------------------------------

void ClockReplacer::Pin(const frame_id_t frame_id) {
  unique_lock scoped_lock(latch_);
  RemoveFrame(frame_id);
}

//-----------------------------------------------------------------------------

void ClockReplacer::Unpin(const frame_id_t frame_id) {
  unique_lock scoped_lock(latch_);

  auto iter = frame_map_.find(frame_id);
  if (iter != frame_map_.end()) {
    return;
  }

  // Add a new frame state since one does not already exist.
  frame_list_.push_back(FrameState(frame_id));
  assert(frame_map_.insert(pair<frame_id_t, list<FrameState>::iterator>(
                            frame_id,
                            prev(frame_list_.end()))).second);

  // If we inserted the first entry set the clock_hand_.
  if (frame_list_.size() == 1) {
    clock_hand_ = frame_list_.begin();
  }
}

//-----------------------------------------------------------------------------

size_t ClockReplacer::Size() {
  shared_lock reader_lock(latch_);
  return frame_list_.size();
}

//-----------------------------------------------------------------------------

void ClockReplacer::RemoveFrame(const frame_id_t frame_id) {
  // TODO(Dan): Implmement mutex that allows us to assert that the current
  //            thread has ownership as should be the case here.
  auto iter = frame_map_.find(frame_id);
  if (iter == frame_map_.end()) {
    // This frame is not being tracked by the clock replacer.
    return;
  }

  // Move the clock hand before deleting the entry if we are deleting the entry
  // the clock hand is pointing towards.
  if (iter->second == clock_hand_) {
    NextClockHand();
  }

  frame_list_.erase(iter->second);
  frame_map_.erase(iter->first);

  // Special case if the list size is now zero the clock hand should be set to
  // end() to indicate no sweeping to be done.
  if (frame_list_.size() == 0) {
    clock_hand_ = frame_list_.end();
  }
}

//-----------------------------------------------------------------------------

void ClockReplacer::NextClockHand() {
  // TODO(Dan): Implmement mutex that allows us to assert that the current
  //            thread has ownership as should be the case here.
  if (clock_hand_ == frame_list_.end()) {
    return;
  }

  ++clock_hand_;
  if (clock_hand_ == frame_list_.end()) {
    clock_hand_ = frame_list_.begin();
  }
}

//-----------------------------------------------------------------------------

}  // namespace bustub
