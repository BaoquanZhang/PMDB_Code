// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "btree_wrapper.h"

namespace leveldb {
std::atomic<uint64_t> write_count{0};
std::atomic<uint64_t> read_count{0};
btree_wrapper global_index;

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();
  // When we build table, we need to record <key, sst, block>
  // so that we can update the global tree index. Therefore,
  // we use three arrays to record them.
  std::vector<std::string> keys;
  std::vector<uint64_t> ssts;
  std::vector<uint64_t> blocks;

  std::string fname = TableFileName(dbname, meta->number);

  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);

    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      std::string real_key = key.ToString().substr(0, 16);
      keys.push_back(real_key);
      ssts.push_back(meta->number);
      // we have to enter table builder to find block offset
      builder->Add(key, iter->value(), blocks);
    }

    // Finish and check for builder errors
    s = builder->Finish(keys, ssts, blocks);
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
    // We update the global tree index here
    global_index.insertKeys(keys, ssts, blocks);
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb
