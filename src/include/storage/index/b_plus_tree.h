//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

#define LEFT 0
#define RIGHT 1

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr);

  // index iterator
  INDEXITERATOR_TYPE begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);
  INDEXITERATOR_TYPE end();

  void Print(BufferPoolManager *bpm) {
    ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
  }

  void Draw(BufferPoolManager *bpm, const std::string &outf) {
    std::ofstream out(outf);
    out << "digraph G {" << std::endl;
    ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
    out << "}" << std::endl;
    out.close();
  }

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);
  // expose for test purpose
  LeafPage *FindLeafPage(const KeyType &key, bool leftMost = false)
  {
    if (IsEmpty()) return nullptr;

    BPlusTreePage *page = GetBPlusPage<BPlusTreePage>(root_page_id_);
    while(!page->IsLeafPage())
    {
      InternalPage *ipage = reinterpret_cast<InternalPage *>(page);
      page_id_t child_id = ipage->Lookup(key, comparator_);

      buffer_pool_manager_->UnpinPage(ipage->GetPageId(), false);
      page = GetBPlusPage<BPlusTreePage>(child_id);
    }

    LeafPage *lpage = reinterpret_cast<LeafPage *>(page);
    return lpage;
  }

 private:
  void StartNewTree(const KeyType &key, const ValueType &value);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  // template <typename N>
  // N *Split(N *node);

  LeafPage *FindLeftSilbing(LeafPage *node)
  {
    page_id_t parent_id =  node->GetParentPageId();
    if (parent_id == INVALID_PAGE_ID) return nullptr;
    
    InternalPage *ipage = GetBPlusPage<InternalPage>(parent_id);
    int index = ipage->ValueIndex(node->GetPageId());
    if (index == 0) return nullptr;

    LeafPage *silb_page = GetBPlusPage<LeafPage>(ipage->ValueAt(index - 1));
    buffer_pool_manager_->UnpinPage(parent_id, false);
    return silb_page;
  }

  LeafPage *FindRightSilbing(LeafPage *node)
  {
    if (node->GetNextPageId() == INVALID_PAGE_ID) return nullptr;

    page_id_t page_id = node->GetNextPageId();
    LeafPage *lpage = GetBPlusPage<LeafPage>(page_id);
    return lpage;
  }

  InternalPage *FindLeftSilbing(InternalPage *node)
  {
    page_id_t parent_id =  node->GetParentPageId();
    if (parent_id == INVALID_PAGE_ID) return nullptr;
    
    InternalPage *ipage = GetBPlusPage<InternalPage>(parent_id);
    int index = ipage->ValueIndex(node->GetPageId());
    if (index == 0) return nullptr;

    InternalPage *silb_page = GetBPlusPage<InternalPage>(ipage->ValueAt(index - 1));
    buffer_pool_manager_->UnpinPage(parent_id, false);
    return silb_page;
  }

  InternalPage *FindRightSilbing(InternalPage *node)
  {
    page_id_t parent_id =  node->GetParentPageId();
    if (parent_id == INVALID_PAGE_ID) return nullptr;
    
    InternalPage *ipage = GetBPlusPage<InternalPage>(parent_id);
    int index = ipage->ValueIndex(node->GetPageId());
    if (index == node->GetSize() - 1) return nullptr;

    InternalPage *silb_page = GetBPlusPage<InternalPage>(ipage->ValueAt(index + 1));
    buffer_pool_manager_->UnpinPage(parent_id, false);
    return silb_page;
  }


  LeafPage *Split(LeafPage *node);

  InternalPage *Split(InternalPage *node);

  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

  // template <typename N>
  // bool Coalesce(N **neighbor_node, N **node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent,
  //               int index, Transaction *transaction = nullptr);

  void Coalesce(InternalPage *left_node, InternalPage *right_node, InternalPage *parent,
                Transaction *transaction = nullptr);

  void Coalesce(LeafPage *left_node, LeafPage *right_node, InternalPage *parent, Transaction *transaction = nullptr);

  void Redistribute(LeafPage *node, LeafPage *neighber_node, InternalPage* parent, int dire);

  void Redistribute(InternalPage *node, InternalPage *neighber_node,
                    InternalPage* parent, KeyType middle_key, int dire);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  /*
   some custom helper method
  */
  template<typename T>
  T *GetBPlusPage(page_id_t page_id)
  {
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) throw "out of memory";

    return reinterpret_cast<T *>(page->GetData());
  }

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
};

}  // namespace bustub
