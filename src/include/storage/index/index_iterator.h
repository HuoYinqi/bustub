//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(page_id_t page_id, int index, BufferPoolManager *bpm);
  IndexIterator(const IndexIterator &ite)
  :index_(ite.index_), leaf_page_(ite.leaf_page_),buffer_pool_manager_(ite.buffer_pool_manager_){};
  // IndexIterator(const IndexIterator &ite)
  // : leaf_page_(ite.leaf_page_), index_(ite.index_), buffer_pool_manager_(ite.buffer_pool_manager_) {}
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const
  {
    return (leaf_page_->GetPageId() == (itr.leaf_page_)->GetPageId()) &&
            (index_ == itr.index_);
  }

  bool operator!=(const IndexIterator &itr) const
  {
    return (leaf_page_->GetPageId() != (itr.leaf_page_)->GetPageId()) ||
            (index_ != itr.index_);
  }

 private:
  // add your own private member variables here
  int index_{-1};
  LeafPage *leaf_page_{nullptr};
  BufferPoolManager *buffer_pool_manager_{nullptr};
};

}  // namespace bustub
