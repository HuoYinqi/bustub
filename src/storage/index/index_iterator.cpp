/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, BufferPoolManager *bpm)
:index_(index), buffer_pool_manager_(bpm)
{
    Page *page = buffer_pool_manager_->WrapFetchPage(page_id);
    if (!reinterpret_cast<BPlusTreePage *>(page->GetData())->IsLeafPage())
    {
        throw "should provide valid leaf page id";
    }
    leaf_page_ = reinterpret_cast<LeafPage *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator()
{
    if (leaf_page_ != nullptr)
    {
        buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), true);
    }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd()
{
    return (leaf_page_->GetNextPageId() == INVALID_PAGE_ID) && (index_ == (leaf_page_->GetSize() - 1));
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*()
{
    return leaf_page_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++()
{
    if (isEnd())
    {
        index_++;
        return *this;
    }
    if (index_ < leaf_page_->GetSize() - 1)
    {
        ++index_;
    }
    else
    {
        index_= -1;
        page_id_t new_page_id =  leaf_page_->GetNextPageId();
        buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), true);
        
        Page *page = buffer_pool_manager_->WrapFetchPage(new_page_id);
        leaf_page_ = reinterpret_cast<LeafPage *>(page->GetData());
        index_ = 0;
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
