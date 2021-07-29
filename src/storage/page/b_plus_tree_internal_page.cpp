//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  int i = 0;
  for(; i < GetSize(); i++)
  {
    if (array[i].second == value) return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { 
  return array[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value)
{
  array[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyValueAt(int index, const KeyType &key, const ValueType &value)
{
  SetKeyAt(index, key);
  SetValueAt(index, value);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  int left = 1;
  int right = GetSize();
  while (left < right)
  {
    int mid = ((right - left) >> 1) + left;
    if (comparator(array[mid].first, key) > 0) right = mid;
    else left = mid + 1;
  }
  return array[left - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
  const ValueType &old_value, const KeyType &new_key,const ValueType &new_value) {
  SetValueAt(0, old_value);
  SetValueAt(1, new_value);
  SetKeyAt(1, new_key);
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter
(const ValueType &old_value, const KeyType &new_key,const ValueType &new_value)
{
  int old_index = ValueIndex(old_value) + 1;
  printf("old_value = %d, old_index = %d\n", old_value, old_index);
  memmove(static_cast<void *>(array + old_index + 1), 
          static_cast<void *>(array + old_index),
          (GetSize() - old_index) * sizeof(MappingType));
  SetKeyValueAt(old_index, new_key, new_value);
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */

// move half to recipient page;

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo
(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int half = GetSize() >> 1;
  if (half == 0) return;


  for(int i = half; i < GetSize(); ++i)
  {
    page_id_t page_id = array[i].second;
    Page *page =buffer_pool_manager->WrapFetchPage(page_id);

    BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *> ((page)->GetData());
    tree_page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(page_id, true);
  }
  memcpy(static_cast<void *> ((recipient->array) + recipient->GetSize()), static_cast<void *> (array + half),
        sizeof(MappingType) * (GetSize() - half));
  recipient->IncreaseSize(GetSize() - half);
  SetSize(half);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  Page *page;
  page_id_t page_id;
  BPlusTreePage *tree_page;
  for (int i = 0; i < size; i ++)
  {
    page_id = items[i].second;
    page = buffer_pool_manager->FetchPage(page_id);
    if (page == nullptr) throw "out of memory";

    tree_page = reinterpret_cast<BPlusTreePage *> (page->GetData());
    tree_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page_id, true);
  }
  memcpy(static_cast<void *>(array + GetSize()), static_cast<void *>(items), sizeof(MappingType) * size);
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int size = GetSize();
  memmove(static_cast<void *>(array + index), static_cast<void *>(array + index + 1),
          sizeof(MappingType) * (size - index - 1));
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  SetSize(0);
  return array[0].second;
 }
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager)
{
  // set middle to first key
  SetKeyAt(0, middle_key);
  int size = GetSize();
  page_id_t page_id;
  Page *page;
  BPlusTreePage *tree_page;
  for(int i = 0; i < size; i++)
  {
    page_id = array[i].second;
    page = buffer_pool_manager->FetchPage(page_id);
    if (page == nullptr) throw "out of memory";

    tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    tree_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page_id, true);
  }
  memcpy(static_cast<void *> ((recipient->array) + recipient->GetSize()), 
        static_cast<void *> (array), sizeof(MappingType) * GetSize());
  recipient->IncreaseSize(GetSize());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager)
{
  recipient->CopyLastFrom(array[0], buffer_pool_manager);
  recipient->SetKeyAt(recipient->GetSize() -1, middle_key);
  memmove(static_cast<void *>(array), static_cast<void *>(array + 1), sizeof(MappingType) * (GetSize() - 1));
  IncreaseSize(-1);
}                                                      

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  page_id_t page_id = pair.second;
  Page *page = buffer_pool_manager->WrapFetchPage(page_id);
  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *> (page->GetData());
  tree_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page_id, true);
  array[GetSize()] = pair;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager)
{
  recipient->CopyFirstFrom(array[GetSize() - 1], buffer_pool_manager);
  recipient->SetKeyAt(1, middle_key);
  IncreaseSize(-1);
}                                                      

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager)
{
  page_id_t page_id = pair.second;
  Page *page = buffer_pool_manager->WrapFetchPage(page_id);
  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *> (page->GetData());
  tree_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page_id, true);

  memmove(static_cast<void *>(array + 1), static_cast<void *>(array), sizeof(MappingType) * GetSize());
  SetKeyValueAt(0, pair.first, pair.second);
  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
