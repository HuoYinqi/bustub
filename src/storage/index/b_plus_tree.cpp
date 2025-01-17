//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction)
{
  if (IsEmpty()) return false;
  
  BPlusTreePage *page = GetBPlusPage<BPlusTreePage>(root_page_id_);

  while(!page->IsLeafPage())
  {
    page_id_t child_id;
    InternalPage *ipage = reinterpret_cast<InternalPage *>(page);
    child_id = ipage->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = GetBPlusPage<BPlusTreePage>(child_id);
  }

  // now tree page is leaf page
  LeafPage *lpage = reinterpret_cast<LeafPage *>(page);
  ValueType value;
  bool ret = lpage->Lookup(key, &value, comparator_);
  buffer_pool_manager_->UnpinPage(lpage->GetPageId(), false);
  if (ret)
  {
    result->push_back(value);
  }
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction)
{
  if (IsEmpty())
  {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->WrapNewPage(&page_id);
  LeafPage *lpage = reinterpret_cast<LeafPage *>(page->GetData());
  lpage->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  lpage->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(page_id, true);
  root_page_id_ = page_id;
  UpdateRootPageId(0);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  BPlusTreePage *page = GetBPlusPage<BPlusTreePage>(root_page_id_);
  while(!page->IsLeafPage())
  {
    InternalPage *ipage = reinterpret_cast<InternalPage *>(page);
    page_id_t child_id = ipage->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(ipage->GetPageId(), false);
    page = GetBPlusPage<BPlusTreePage>(child_id);
  }

  // now we find leaf page
  LeafPage *lpage = reinterpret_cast<LeafPage *>(page);
  
  ValueType tmp;
  bool ret = lpage->Lookup(key, &tmp, comparator_);
  if (ret) return false;

  // now we can insert key/value
  lpage->Insert(key, value, comparator_);
  
  // judge whether leaf page is full
  if (lpage->IsFull())
  {
    LeafPage * new_leaf_page = Split(lpage);
    InsertIntoParent(lpage, new_leaf_page->KeyAt(0), new_leaf_page);
  }
  buffer_pool_manager_->UnpinPage(lpage->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */

INDEX_TEMPLATE_ARGUMENTS
typename BPLUSTREE_TYPE::LeafPage *BPLUSTREE_TYPE::Split(LeafPage *old_leaf_page) {
  page_id_t page_id;
  LeafPage *new_leaf_page = reinterpret_cast<LeafPage *>((buffer_pool_manager_->WrapNewPage(&page_id))->GetData());
  new_leaf_page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  old_leaf_page->MoveHalfTo(new_leaf_page);
  new_leaf_page->SetNextPageId(old_leaf_page->GetNextPageId());
  old_leaf_page->SetNextPageId(new_leaf_page->GetPageId());
  return new_leaf_page;
}

INDEX_TEMPLATE_ARGUMENTS
typename BPLUSTREE_TYPE::InternalPage *BPLUSTREE_TYPE::Split(InternalPage *old_internal_page) {
  page_id_t page_id;
  InternalPage *new_page = reinterpret_cast<InternalPage *>((buffer_pool_manager_->WrapNewPage(&page_id))->GetData());
  new_page->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
  old_internal_page->MoveHalfTo(new_page, buffer_pool_manager_);
  return new_page;
}

// INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
// N *BPLUSTREE_TYPE::Split(N *node) {
//   // BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(node);
//   page_id_t page_id;
//   N* new_page =reinterpret_cast<N *>((buffer_pool_manager_->WrapNewPage(&page_id))->GetData());
//   new_page->SetPageId(page_id);
//   if (node->IsLeafPage())
//   {
//     LeafPage *leaf_page = reinterpret_cast<LeafPage *>(node);
//     leaf_page->MoveHalfTo(new_page);
//     new_page->SetNextPageId(leaf_page->GetNextPageId());
//     leaf_page->SetNextPageId(new_page->GetPageId());
//   }
//   else
//   {
//     InternalPage *int_page = reinterpret_cast<InternalPage *>(node);
    
//     int_page->MoveHalfTo(new_page, buffer_pool_manager_);
//   }
//   return new_page;
// }

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction)
{
  InternalPage *parent_page;
  page_id_t parent_id = old_node->GetParentPageId();

  if (parent_id == INVALID_PAGE_ID)
  {
    page_id_t parent_page_id;
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(
      (buffer_pool_manager_->WrapNewPage(&parent_page_id)) ->GetData()
    );
    parent_page->Init(parent_page_id, INVALID_PAGE_ID, internal_max_size_);
    parent_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent_page_id);
    old_node->SetParentPageId(parent_page_id);
    root_page_id_ = parent_page_id;
    UpdateRootPageId();

    buffer_pool_manager_->UnpinPage(parent_page_id, true);
  }

  else{
    parent_page = GetBPlusPage<InternalPage>(old_node->GetParentPageId());
    
    parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent_page->GetPageId());

    if (parent_page->GetSize() > parent_page->GetMaxSize()) // split it;
    {
      InternalPage *split_page = Split(parent_page);
      KeyType new_key = split_page->KeyAt(0);
      InsertIntoParent(parent_page, new_key, split_page, transaction);
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  LeafPage *lpage = FindLeafPage(key);
  if(lpage == nullptr) return;

  lpage->RemoveAndDeleteRecord(key, comparator_);

  if (lpage->GetSize() < lpage->GetMinSize())
  {
    CoalesceOrRedistribute(lpage, transaction);
  }

}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {

  BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(node);
  if (page->IsRootPage())
  {
    if (AdjustRoot(page))
    {
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      return true;
    }
    return false;
    // TODO
  }
  if (page->IsLeafPage()) // when N is LeafPage
  {
    LeafPage *lpage = reinterpret_cast<LeafPage *>(page);
    LeafPage *silb_page;

    InternalPage *parent_page;

    if (lpage->GetNextPageId() == -1)     // indicate lpage is the most right leaf, find left silbing page
    {
      silb_page = FindLeftSilbing(lpage);
      if (silb_page == nullptr) 
      {
        return false;
      }
      parent_page = GetBPlusPage<InternalPage>(lpage->GetParentPageId());
      if (silb_page->GetSize() + lpage->GetSize() <= leaf_max_size_)
      {
        Coalesce(silb_page, lpage, parent_page, transaction);
      }
      else
      {
        Redistribute(lpage, silb_page, parent_page, LEFT);
      }
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    }
    else                                  // to find right silbing node
    {
      silb_page = FindRightSilbing(lpage);
      if (silb_page == nullptr)
      {
        return false;
      }
      parent_page = GetBPlusPage<InternalPage>(silb_page->GetParentPageId());
      if (silb_page->GetSize() + lpage->GetSize() <= leaf_max_size_)
      {
        Coalesce(lpage, silb_page, parent_page, transaction);
      }
      else
      {
        Redistribute(lpage, silb_page, parent_page, RIGHT);
      }
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    }
  }

  else
  {
    InternalPage *ipage = reinterpret_cast<InternalPage *>(page);
    InternalPage *silb_page;
    InternalPage *parent_page = GetBPlusPage<InternalPage>(ipage->GetParentPageId());
    KeyType middle_key;
    if (parent_page->ValueIndex(ipage->GetPageId()) == parent_page->GetSize() - 1)
    {
      silb_page = FindLeftSilbing(ipage);
      if (silb_page == nullptr)
      {
        return false;
      }
      if (silb_page->GetSize() + ipage->GetSize() < internal_max_size_)
      {
        Coalesce(silb_page, ipage, parent_page, transaction);
      }
      else
      {
        int parent_index = parent_page->ValueIndex(ipage->GetPageId());
        middle_key = parent_page->KeyAt(parent_index);
        Redistribute(ipage, silb_page, parent_page, middle_key, LEFT);
        parent_page->SetKeyAt(parent_index, ipage->KeyAt(0));
      }
    }
    else                                  // to find right silbing node
    {
      silb_page = FindRightSilbing(ipage);
      if (silb_page == nullptr)
      {
        return false;
      }
      if (silb_page->GetSize() + ipage->GetSize() < internal_max_size_)
      {
        Coalesce(ipage, silb_page, parent_page, transaction);
      }
      else
      {
        int parent_index = parent_page->ValueIndex(silb_page->GetPageId());
        middle_key = parent_page->KeyAt(parent_index);
        Redistribute(ipage, silb_page, parent_page, middle_key, RIGHT);
        parent_page->SetKeyAt(parent_index, silb_page->KeyAt(0));
      }
    }
  }
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Coalesce(InternalPage *left_node, InternalPage *right_node, InternalPage *parent,
                Transaction *transaction)
{
  int index = parent->ValueIndex(right_node->GetPageId());
  right_node->SetKeyAt(0, parent->KeyAt(index));
  right_node->MoveAllTo(left_node, right_node->KeyAt(0), buffer_pool_manager_);
  
  parent->Remove(index);
  if (parent->GetSize() < parent->GetMinSize())
  {
    CoalesceOrRedistribute(parent, transaction);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Coalesce(LeafPage *left_node, LeafPage *right_node, InternalPage *parent,
                      Transaction *transaction)
{
  int index = parent->ValueIndex(right_node->GetPageId());
  right_node->MoveAllTo(left_node);
  left_node->SetNextPageId(right_node->GetNextPageId());
  parent->Remove(index);

  if (parent->GetSize() < parent->GetMinSize())
  {
    CoalesceOrRedistribute(parent, transaction);
  }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(LeafPage *node, LeafPage *neighber_node, InternalPage* parent, int dire)
{
  if (dire == LEFT)
  {
    neighber_node->MoveLastToFrontOf(node);
    parent->SetKeyAt(parent->ValueIndex(node->GetPageId()), node->KeyAt(0));
  }
  else
  {
    neighber_node->MoveFirstToEndOf(node);
    parent->SetKeyAt(parent->ValueIndex(neighber_node->GetPageId()), neighber_node->KeyAt(0));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(InternalPage *node, InternalPage *neighber_node,
                                  InternalPage* parent, KeyType middle_key, int dire)
{
  if (dire == LEFT)
  {
    neighber_node->MoveLastToFrontOf(node, middle_key, buffer_pool_manager_);
    parent->SetKeyAt(parent->ValueIndex(node->GetPageId()), node->KeyAt(0));
  }
  else
  {
    neighber_node->MoveFirstToEndOf(node, middle_key, buffer_pool_manager_);
    parent->SetKeyAt(parent->ValueIndex(neighber_node->GetPageId()), neighber_node->KeyAt(0));
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node)
{
  if (old_root_node->GetSize() == 1 && !old_root_node->IsLeafPage())
  {
    page_id_t new_root_page_id = (reinterpret_cast<InternalPage *>(old_root_node))->ValueAt(0);
    root_page_id_ = new_root_page_id;
    BPlusTreePage *new_root_page = GetBPlusPage<BPlusTreePage>(new_root_page_id);
    new_root_page->SetParentPageId(INVALID_PAGE_ID);
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    return true;
  }

  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  if (IsEmpty()) return INDEXITERATOR_TYPE(INVALID_PAGE_ID, -1, nullptr);
  BPlusTreePage *page = GetBPlusPage<BPlusTreePage>(root_page_id_);
  while(!page->IsLeafPage())
  {
    InternalPage *ipage = reinterpret_cast<InternalPage *>(page);
    page_id_t child_id = ipage->ValueAt(0);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = GetBPlusPage<BPlusTreePage>(child_id);
  }

  LeafPage *lpage =reinterpret_cast<LeafPage *>(page);
  page_id_t page_id  = lpage->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return INDEXITERATOR_TYPE(page_id, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  LeafPage *lpage = FindLeafPage(key);
  page_id_t page_id = lpage->GetPageId();
  int index;
  int left = 0;
  int right = lpage->GetSize();
  while(left < right)
  {
    int mid = ((right - left) >> 1) + left;
    if (comparator_(lpage->KeyAt(mid), key) == 0)
    {
      index =  mid;
      break;
    }
    else if (comparator_(lpage->KeyAt(mid), key) > 0)
    {
      right = mid;
    }
    else left = mid + 1;
  }
  buffer_pool_manager_->UnpinPage(lpage->GetPageId(), false);
  return INDEXITERATOR_TYPE(page_id, index, buffer_pool_manager_);
}



/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end()
{
  if (IsEmpty()) return INDEXITERATOR_TYPE(INVALID_PAGE_ID, -1, nullptr);
  BPlusTreePage *page = GetBPlusPage<BPlusTreePage>(root_page_id_);
  while(!page->IsLeafPage())
  {
    InternalPage *ipage = reinterpret_cast<InternalPage *>(page);
    page_id_t child_id = ipage->ValueAt(ipage->GetSize() - 1);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = GetBPlusPage<BPlusTreePage>(child_id);
  }

  LeafPage *lpage =reinterpret_cast<LeafPage *>(page);
  page_id_t page_id  = lpage->GetPageId();
  int index = lpage->GetSize();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return INDEXITERATOR_TYPE(page_id, index, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
// INDEX_TEMPLATE_ARGUMENTS
// Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost)
// {
//   // ignore leftMost templately

//   if (IsEmpty()) return nullptr;

//   BPlusTreePage *page = GetBPlusPage<BPlusTreePage>(root_page_id_);
//   while(!page->IsLeafPage())
//   {
//     InternalPage *ipage = reinterpret_cast<InternalPage *>(page);
//     page_id_t child_id = ipage->Lookup(key, comparator_);

//     buffer_pool_manager_->UnpinPage(ipage->GetPageId());
//     page = GetBPlusPage<BPlusTreePage>(child_id);
//   }

//   LeafPage *lpage = reinterpret_cast<LeafPage *>(page);
//   return lpage;
  
// }

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
