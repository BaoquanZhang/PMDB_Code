//
// Created by Baoquan Zhang on 2/4/20.
//

#include "btree_wrapper.h"

#include "db/db_impl.h"
#include "db/version_set.h"
#include <chrono>
#include <iostream>
#include <thread>
#include <unordered_set>

#include "leveldb/db.h"

namespace leveldb {

FileMetaData* find_filemeta(uint64_t sid, Version* v) {
  auto file_metas = v->filemeta();
  for (int i = 0; i < config::kNumLevels; i++) {
    for (uint64_t j = 0; j < file_metas[i].size(); j++) {
      if (file_metas[i][j]->number == sid) {
        return file_metas[i][j];
      }
    }
  }
  return nullptr;
}

bool btree_wrapper::findKey(std::string key,
                            std::pair<uint64_t, uint64_t>& sst_offset) {
  uint64_t cur_reads = std::log(global_tree.size());
  std::this_thread::sleep_for(
      std::chrono::nanoseconds(nvm_read_latency_ns_ * cur_reads));
  mem_reads_ += cur_reads;
  auto it = global_tree.find(key);
  if (it == global_tree.end()) {
    return false;
  }
  sst_offset = it->second;
  return true;
}

void btree_wrapper::insertKey(std::string key, uint64_t sst_id,
                              uint64_t block_offset) {
  // insert the key to position
  // all keys after position will be shift
  std::pair<uint64_t, uint64_t> sst_offset(sst_id, block_offset);
  std::pair<const std::string, std::pair<uint64_t, uint64_t>> entry(key,
                                                                    sst_offset);
  auto it = global_tree.lower_bound(key);
  if (it != global_tree.end() && it->first == key) {
    // update live ratio of sst
    uint64_t sid = it->second.first;
    // sst_live_ratio[sst_id]++;
    sst_valid_key[sid].second--;
    int liveratio = (int)(sst_valid_key[sid].first / sst_valid_key[sid].second);
    if (liveratio >= liveratio_threshold) {
      candidate_list_ssts.emplace(sst_id);
    }
    it->second.first = sst_id;
    it->second.second = block_offset;
    mem_reads_++;
  } else {
    global_tree.insert(it, entry);
  }
  mem_writes_++;
}

void btree_wrapper::insertKeys(std::vector<std::string> keys,
                               std::vector<uint64_t> ssts,
                               std::vector<uint64_t> blocks) {
  assert(keys.size() > 0);
  assert(keys.size() == ssts.size());
  assert(keys.size() == blocks.size());
  std::pair<uint64_t, uint64_t> valid_invalid(keys.size(), 0);
  std::pair<uint64_t, std::pair<uint64_t, uint64_t>> entry(ssts[0],
                                                           valid_invalid);
  sst_valid_key.insert(entry);

  std::vector<std::pair<const std::string, std::pair<uint64_t, uint64_t>>>
      entries;
  entries.reserve((keys.size()));

  for (uint64_t i = 0; i < keys.size(); i++) {
    entries.emplace_back(keys[i], std::make_pair(ssts[i], blocks[i]));
  }

  auto it = global_tree.lower_bound(keys[0]);
  // mem_reads_ += std::log(global_tree.size());
  int cur_key_pos = 0;
  while (it != global_tree.end() && cur_key_pos < keys.size()) {
    if (it->first == keys[cur_key_pos]) {
      // update live ratio of sst
      uint64_t sid = it->second.first;
      // sst_live_ratio[sst_id]++;
      sst_valid_key[sid].second--;
      int liveratio =
          (int)(sst_valid_key[sid].first / sst_valid_key[sid].second);
      if (liveratio >= liveratio_threshold) {
        candidate_list_ssts.emplace(sid);
      }
      it->second.first = ssts[cur_key_pos];
      it->second.second = blocks[cur_key_pos];
      cur_key_pos++;
      it++;
    } else if (it->first < keys[cur_key_pos]) {
      it++;
    } else {
      it = global_tree.insert(it, entries[cur_key_pos]);
      cur_key_pos++;
      it++;
    }
    mem_writes_++;
  }

  while (cur_key_pos < keys.size()) {
    global_tree.insert(global_tree.end(), entries[cur_key_pos]);
    cur_key_pos++;
    mem_writes_++;
  }

  /*
  for (uint64_t i = 0; i < keys.size(); i++) {
    insertKey(keys[i], ssts[i], blocks[i]);
  }
  */
  std::this_thread::sleep_for(std::chrono::nanoseconds(nvm_write_latency_ns_));
}

// TODO() what if key is not exist? should use the next key no less than
// cur_key
std::string btree_wrapper::scanLeafnode(std::string cur_key, uint64_t num) {
  if (global_tree.size() == 0) return "";
  std::unordered_set<int> unique_file_id;
  auto it = global_tree.begin();
  if (!cur_key.empty()) it = global_tree.upper_bound(cur_key);
  // auto it = global_tree.begin();
  uint64_t read_counts = std::log(global_tree.size());
  std::this_thread::sleep_for(
      std::chrono::nanoseconds(nvm_read_latency_ns_ * read_counts));
  std::string next_key;
  while (it != global_tree.end()) {
    for (uint64_t i = 0; i < num && it != global_tree.end(); it++, i++) {
      unique_file_id.emplace(it->second.first);
      mem_reads_++;
    }
    if (unique_file_id.size() >= leafnodescan_threshold) {
      candidate_list_ssts.insert(unique_file_id.begin(), unique_file_id.end());
    }
    unique_file_id.clear();
    if (candidate_list_ssts.size() >= candidate_list_size) break;
  }
  next_key = (it == global_tree.end()) ? global_tree.begin()->first : it->first;
  return next_key;
}

uint64_t btree_wrapper::findSid(std::string key) {
  auto it = global_tree.find(key);
  std::this_thread::sleep_for(std::chrono::nanoseconds(nvm_read_latency_ns_));
  mem_reads_++;
  if (it == global_tree.end()) {
    return -1;
  } else {
    std::pair<uint64_t, uint64_t> entry = it->second;
    return entry.first;
  }
}

}
