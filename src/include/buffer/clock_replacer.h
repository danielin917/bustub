//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.h
//
// Identification: src/include/buffer/clock_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/*
 * Clock Replacer class that implements a clock sweep algorithm to keep track
 * of victim frames to be evicted.
 *
 * This class is thread safe.
 *
 */

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * ClockReplacer implements the clock replacement policy, which approximates
 * the Least Recently Used policy.
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be
   * required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

  // Removes next victim from clock replacer. 'frame_id' set to the id of the
  // removed frame if a victim was found. Returns true if successful, false
  // otherwise.
  bool Victim(frame_id_t *frame_id) override;

  // Removes frame with 'frame_id' from replacer as it can no longer become a
  // victim.
  void Pin(frame_id_t frame_id) override;

  // Adds frame state for frame with 'frame_id' to replacer as it can now
  // become a victim.
  void Unpin(frame_id_t frame_id) override;

  // Returns the number of frames being tracked by the replacer.
  size_t Size() override;

 private:
  //  Removes FrameState associated with 'frame_id' from all internal
  //  datastructures.
  void RemoveFrame(frame_id_t frame_id);

  // Moves clock hand to the next item.
  void NextClockHand();

  // State containing relevant information about a specific frame.
  struct FrameState {
    // Constructor.
    explicit FrameState(const frame_id_t id) :
      frame_id(id),
      reference_bit(true) {
    }

    // Frame id for this frame.
    frame_id_t frame_id;

    // Reference bit for clock sweep algorithm.
    bool reference_bit;
  };

 private:
  // Mutex used to protect the underlying variables.
  std::shared_mutex latch_;

  // Frame map maping a frame id to the frame's current location in the list
  // which gives the frame state itself.
  std::unordered_map<frame_id_t, std::list<FrameState>::iterator> frame_map_;

  // Frane list containing an ordered list of frame states for our clock sweep
  // algorithm. New frames will be added to the back of the list.
  std::list<FrameState> frame_list_;

  // Iterator containing the next frame to be examined in the clock sweep
  // algorithm.
  std::list<FrameState>::iterator clock_hand_;
};

}  // namespace bustub
