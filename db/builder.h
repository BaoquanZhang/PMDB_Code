// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_BUILDER_H_
#define STORAGE_LEVELDB_DB_BUILDER_H_

#include "leveldb/status.h"

namespace leveldb {

struct Options;
struct FileMetaData;

class Env;
class Iterator;
class TableCache;
class VersionEdit;

// Build a Table file from the contents of *iter.  The generated file
// will be named according to meta->number.  On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta);

// Build a table by a stopper. the stopper can be a key or a size.
// A 0 stop_size means to stop building the table by a key. If the key in
// the iterator is larger than the stop key, we stop building the table.
// If we set the stop_size, we will stop building a table when the size of the
// table is not smaller than the stop size.
Status BuildTableForPartitions(const std::string& dbname, Env* env, const Options& options,
                           TableCache* table_cache, Iterator* iter, FileMetaData* meta);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_BUILDER_H_
