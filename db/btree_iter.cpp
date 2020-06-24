//
// Created by Haoyu Gong.
//
#include "btree_iter.h"
#include "filename.h"
#include "db/db_impl.h"

namespace leveldb{
Iterator* NewBtreeIter(DBImpl* db,const ReadOptions& options){
  return new BtreeIter(db, options);
}

BtreeIter::~BtreeIter() {
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
    std::string key = tree_iter_->first;
    auto entry = tree_iter_->second;
    uint64_t sst_id = entry.sid;
    uint64_t block_offset = entry.block_offset;
    uint64_t block_size = entry.block_size;
    if (block_contents_[sst_id].count(block_offset) == 0) {
      // the block has never been read.
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
      block_handle.set_offset(block_offset);
      block_handle.set_size(block_size);
      s = ReadBlock(file, options_, block_handle,
                    &block_contents_[sst_id][block_offset]);
      if (!s.ok()) {
        delete file;
        return s;
      }
    }
    delete cur_block_;
    delete cur_block_iter_;
    cur_block_ = new Block(block_contents_[sst_id][block_offset]);
    cur_block_iter_ = cur_block_->NewIterator(db_->options_.comparator);
    cur_block_iter_->Seek(Slice(tree_iter_->first));
  }
  return s;
}

void BtreeIter::Seek(const Slice& target){
  tree_iter_ = global_index.seek(target);
  if (tree_iter_ != global_index.seektoEnd()) {
    UpdateCurBlockIter();
  }
}

void BtreeIter::Next() {
  if(tree_iter_ != global_index.seektoEnd()){
    tree_iter_++;
    if (tree_iter_ != global_index.seektoEnd()) {
      UpdateCurBlockIter();
    }
  }
}

void BtreeIter::Prev() {
  if(tree_iter_ != global_index.seektoFirst()) {
    tree_iter_--;
    if (tree_iter_ != global_index.seektoEnd()) {
      UpdateCurBlockIter();
    }
  }
}

Slice BtreeIter::key() const {
  return tree_iter_->first;
}

bool BtreeIter::Valid() const {
  return cur_block_iter_!= nullptr
         && cur_block_iter_->Valid();
}

Slice BtreeIter::value() const {
  return cur_block_iter_->value();
}
Status BtreeIter::status() const {
  // do nothing
  return cur_block_iter_->status();
}
}
