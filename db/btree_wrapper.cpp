//
// Created by Baoquan Zhang on 2/4/20.
//

#include "btree_wrapper.h"
#include <iostream>
namespace leveldb {
  bool btree_wrapper::findKey(std::string key, std::pair<uint64_t, uint64_t>& sst_offset) {
    auto it = global_tree.find(key);
    if (it == global_tree.end()) {
      return false;
    }
    sst_offset = it->second;
    return true;
  }

  void btree_wrapper::insertKey(std::string key, uint64_t sst_id, uint64_t block_offset) {
    // insert the key to position
    // all keys after position will be shift
    std::pair<uint64_t, uint64_t> sst_offset(sst_id, block_offset);
    std::pair<const std::string, std::pair<uint64_t, uint64_t>> entry(key, sst_offset);
    global_tree.insert(entry);
  }

  void btree_wrapper::insertKeys(std::vector<std::string> keys, std::vector<uint64_t> ssts,
                                 std::vector<uint64_t> blocks) {
    assert(keys.size() == ssts.size());
    assert(keys.size() == blocks.size());
    for (uint64_t i = 0; i < keys.size(); i++) {
      insertKey(keys[i], ssts[i], blocks[i]);
    }
  }
}