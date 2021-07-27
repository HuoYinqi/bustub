//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

// LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

LRUReplacer::LRUReplacer(size_t num_pages)
{
  max_pages = num_pages;
  cur_pages = 0;
}

bool LRUReplacer::Victim(frame_id_t *frame_id)
{
  if (cur_pages == 0) return false;
  *frame_id = frame_ids.back();
  frame_ids.pop_back();
  cur_pages --;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id)
{
  auto iter = std::find(frame_ids.begin(), frame_ids.end(), frame_id);
  if(iter == frame_ids.end()) return;
  
  frame_ids.erase(iter);
  cur_pages --;
}

void LRUReplacer::Unpin(frame_id_t frame_id)
{
  auto iter = std::find(frame_ids.begin(), frame_ids.end(), frame_id);
  if (iter != frame_ids.end())
  {
    return;
  }
  if (cur_pages == max_pages)
  {
    frame_id_t vict_id;
    Victim(&vict_id);
  }
  frame_ids.push_front(frame_id);
  cur_pages ++;
}
size_t LRUReplacer::Size()
{
  return cur_pages;
};


}  // namespace bustub
