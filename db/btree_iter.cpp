//
// Created by Haoyu Gong.
//
#include "btree_iter.h"
#include "filename.h"
#include "table/format.h"
#include "table/block.h"
namespace leveldb{
Iterator* NewBtreeIter(DBImpl* db,const ReadOptions& options){
  return new BtreeIter(db,options);
}

void BtreeIter::SeekToFirst(){
  tree_iter = global_index.seektoFirst();
}

void BtreeIter::SeekToLast(){
  tree_iter = global_index.seektoLast();
}

void BtreeIter::Seek(const Slice& target){
  tree_iter = global_index.seek(target);
}

void BtreeIter::Next() {
  if(tree_iter != global_index.seektoEnd()){
    tree_iter++;
  }
}

void BtreeIter::Prev() {
  if(tree_iter != global_index.seektoFirst()){
    tree_iter--;
  }
}

Slice BtreeIter::key() const {
  return global_index.key(tree_iter);
}

bool BtreeIter::Valid() const {
  return tree_iter != global_index.seektoEnd();
}

Slice BtreeIter::value() const {
  Status s;
  std::string value;
  Slice key = this->key();
  std::string key_str = key.ToString().substr(0,16);
  std::pair<uint64_t, uint64_t> sst_offset;
  if(global_index.findKey(key_str,sst_offset)){
    uint64_t file_size;
    RandomAccessFile *file = nullptr;
    std::string fname = TableFileName(db_->dbname_, sst_offset.first);
    s = db_->env_->GetFileSize(fname, &file_size);
    if (s.ok()) {
      s = db_->env_->NewRandomAccessFile(fname, &file);
    }

    if (!s.ok()) {
      delete file;
      return Slice();
    }
    BlockHandle block_handle;
    block_handle.set_offset(sst_offset.second);
    block_handle.set_size(db_->options_.block_size);
    BlockContents block_contents;
    s = ReadBlock(file, options_, block_handle, &block_contents);
    if (!s.ok()) {
      return Slice();
    }
    // create a block in memory using the block content
    Block* block = new Block(block_contents);

    // Construct a block iterator for the block
    auto block_iter = block->NewIterator(db_->options_.comparator);

    // Seek the key using the block iterator
    block_iter->Seek(key);
    if (block_iter->Valid() && block_iter->key().compare(key) == 0) {
      // we find the key
      const char* value_data = block_iter->value().data();
      uint64_t value_size = block_iter->value().size();
      value.assign(value_data, value_size);
    }
    s = block_iter->status();
    // delete the block iterator after finishing using it
    delete block_iter;
    return Slice(value);
  }
  return Slice();
}
}
