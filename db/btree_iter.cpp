//
// Created by Haoyu Gong.
//
#include "btree_iter.h"
#include "filename.h"
#include "table/format.h"
#include "db/db_impl.h"

namespace leveldb{
Iterator* NewBtreeIter(DBImpl* db,const ReadOptions& options){
  return new BtreeIter(db, options);
}

BtreeIter::~BtreeIter() {
  //delete all stored block iters
  for (auto file_it = block_iters_.begin();
       file_it != block_iters_.end();
       file_it++) {
    for (auto offset_it = file_it->second.begin();
         offset_it != file_it->second.end();
         offset_it++) {
      delete offset_it->second;
    }
  }
}

void BtreeIter::SeekToFirst(){
  tree_iter_ = global_index.seektoFirst();
}


void BtreeIter::SeekToLast(){
  tree_iter_ = global_index.seektoLast();
}

Status BtreeIter::UpdateCurBlockIter() {
  Status s = Status::IOError("updating block iter failed!");
  if(tree_iter_ != global_index.seektoEnd()) {
    auto sst_offset = tree_iter_->second;
    uint64_t sst_id = sst_offset.first;
    uint64_t file_offset = sst_offset.second;
    if (block_iters_[sst_id][file_offset] == nullptr) {
      uint64_t file_size;
      RandomAccessFile* file = nullptr;
      std::string fname = TableFileName(db_->dbname_, sst_id);
      s = db_->env_->GetFileSize(fname, &file_size);
      if (s.ok()) {
        s = db_->env_->NewRandomAccessFile(fname, &file);
      }
      if (!s.ok()) {
        delete file;
        return s;
      }
      BlockHandle block_handle;
      block_handle.set_offset(file_offset);
      block_handle.set_size(db_->options_.block_size);
      BlockContents block_contents;
      /* Read block failed. */
      s = ReadBlock(file, options_, block_handle, &block_contents);
      if (!s.ok()) {
        delete file;
        return s;
      }
      // create a block in memory using the block content
      Block* block = new Block(block_contents);
      // Construct a block iterator for the block
      block_iters_[sst_id][file_offset] =
          block->NewIterator(db_->options_.comparator);
      delete file;
      delete block;
    }
    cur_block_iter_ = block_iters_[sst_id][file_offset];
  }
  return s;
}

void BtreeIter::Seek(const Slice& target){
  tree_iter_ = global_index.seek(target);
  UpdateCurBlockIter();
  if (cur_block_iter_ && cur_block_iter_->Valid()) {
    cur_block_iter_->Seek(target);
  }
}

void BtreeIter::Next() {
  if(tree_iter_ != global_index.seektoEnd()){
    tree_iter_++;
    UpdateCurBlockIter();
    if (cur_block_iter_ && cur_block_iter_->Valid()) {
      cur_block_iter_++;
    }
  }
}

void BtreeIter::Prev() {
  if(tree_iter_ != global_index.seektoFirst()){
    tree_iter_--;
    UpdateCurBlockIter();
    if (cur_block_iter_ && cur_block_iter_->Valid()) {
      cur_block_iter_--;
    }
  }
}

Slice BtreeIter::key() const {
  return global_index.key(tree_iter_);
}

bool BtreeIter::Valid() const {
  return tree_iter_ != global_index.seektoEnd()
      && cur_block_iter_ && cur_block_iter_->Valid();
}

Slice BtreeIter::value() const {
  return cur_block_iter_->value();
}
Status BtreeIter::status() const {
  // do nothing
  return cur_block_iter_->status();
}
}
