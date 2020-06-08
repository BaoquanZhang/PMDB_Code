//
// Created by Haoyu Gong.
//

#ifndef LEVELDB_BTREE_ITER_H
#define LEVELDB_BTREE_ITER_H

#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "db/btree_wrapper.h"
namespace leveldb{

class DBImpl;

class BtreeIter : public Iterator{
 public:
  BtreeIter(DBImpl* db, const ReadOptions& options) : db_(db),options_(options){}
  BtreeIter(const BtreeIter&) = delete;
  BtreeIter& operator=(const BtreeIter&) = delete;
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
  btree::btree_map<std::string, std::pair<uint64_t, uint64_t>>::iterator tree_iter;
  DBImpl* db_;
  ReadOptions options_;
  Status status_;
};

Iterator* NewBtreeIter(DBImpl* db, const ReadOptions& options);

}
#endif  // LEVELDB_BTREE_ITER_H
