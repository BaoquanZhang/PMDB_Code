//
// Created by Haoyu Gong.
//

#ifndef LEVELDB_BTREE_ITER_H
#define LEVELDB_BTREE_ITER_H

#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "db/btree_wrapper.h"
#include "table/block.h"
#include "table/format.h"

#include <unordered_map>

namespace leveldb{

class DBImpl;

class BtreeIter : public Iterator{
 public:
  BtreeIter(DBImpl* db, const ReadOptions& options)
      : db_(db), options_(options), cur_block_(nullptr), cur_block_iter_(nullptr){}
  BtreeIter(const BtreeIter&) = delete;
  BtreeIter& operator=(const BtreeIter&) = delete;
  ~BtreeIter();
  Status UpdateCurBlockIter();
  void Next() override;
  void Prev() override;
  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  Slice key() const override;
  Slice value() const override;
  Status status() const override;
  bool Valid() const override;

 private:
  //btree::btree_map<std::string, std::pair<uint64_t, uint64_t>>::iterator tree_iter_;
  btree::btree_map<std::string, leafnode>::iterator tree_iter_;
  DBImpl* db_;
  ReadOptions options_;
  // the keys are sst_id and sst_offset
  std::unordered_map<uint64_t,
                     std::unordered_map<uint64_t, BlockContents>> block_contents_;
  Block* cur_block_;
  Iterator* cur_block_iter_;
};

Iterator* NewBtreeIter(DBImpl* db, const ReadOptions& options);

}
#endif  // LEVELDB_BTREE_ITER_H
