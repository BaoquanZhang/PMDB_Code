//
// Created by Baoquan Zhang on 2/4/20.
//

#include "btree_wrapper.h"
#include <iostream>
#include <unordered_set>

#include "leveldb/db.h"
#include "db/version_set.h"
#include "db/db_impl.h"

#include <atomic>
#include <cmath>

namespace leveldb {

  FileMetaData* find_filemeta(uint64_t sid, std::vector<FileMetaData*>** files_){
    std::vector<FileMetaData*> *fs = *files_;
    for(int i = 0; i < config::kNumLevels; i++){
      std::vector<FileMetaData*> tmp = fs[i];
      for(auto it = tmp.begin(); it != tmp.end(); it++){
        if((*it)->number == sid ){
          return (*it);
        }
      }
    }
    return nullptr;
  }

  bool btree_wrapper::findKey(std::string key, std::pair<uint64_t, uint64_t>& sst_offset) {
    mem_reads_ += std::log(global_tree.size());
    auto it = global_tree.find(key);
    if (it == global_tree.end()) {
      return false;
    }
    sst_offset = it->second;
    return true;
  }

  void btree_wrapper::insertKey(std::string key, uint64_t sst_id, uint64_t block_offset,std::vector<FileMetaData*>** files_) {
    // insert the key to position
    // all keys after position will be shift
    std::pair<uint64_t, uint64_t> sst_offset(sst_id, block_offset);
    std::pair<const std::string, std::pair<uint64_t, uint64_t>> entry(key, sst_offset);
    auto it = global_tree.find(key);
    if (it != global_tree.end()) {
      // update live ratio of sst
      uint64_t sid = it->second.first;
      //sst_live_ratio[sst_id]++;
      sst_valid_key[sid].second--;
      int liveratio = (int)(sst_valid_key[sid].first / sst_valid_key[sid].second);
      if(liveratio >= liveratio_threshold){
       FileMetaData* f; //find meta data by sst_id
       f = find_filemeta(sid,files_);
       if(f){
         mtx_.Lock();
         candidate_list_ssts[sid]=f;
         mtx_.Unlock();
       }      
      }
    }
    mem_writes_++;
    global_tree.insert(it, entry);
  }

  void btree_wrapper::insertKeys(std::vector<std::string> keys, std::vector<uint64_t> ssts,
                                 std::vector<uint64_t> blocks,std::vector<FileMetaData*>** files_) {
    if (keys.size() == 0)
      return;
    assert(keys.size() == ssts.size());
    assert(keys.size() == blocks.size());
    std::pair<uint64_t,uint64_t> valid_invalid(keys.size(),0);
    std::pair<uint64_t,std::pair<uint64_t,uint64_t>> entry(ssts[0],valid_invalid);
    sst_valid_key.insert(entry);
    mtx_.Lock();
    for (uint64_t i = 0; i < keys.size(); i++) {
      insertKey(keys[i], ssts[i], blocks[i],files_);
    }
    mtx_.Unlock();
  }

//TODO() what if key is not exist? should use the next key no less than cur_key
  std::string btree_wrapper::scanLeafnode(std::string cur_key, int num,std::vector<FileMetaData*>** files_){
    if (global_tree.size() == 0)
      return "";
    std::unordered_set<int> unique_file_id;
    auto it = global_tree.upper_bound(cur_key);
    mem_reads_ += std::log(global_tree.size());
    //auto it = global_tree.find(cur_key);
    std::string next_key;
    for(int i = 0; i < num && it != global_tree.end(); it++, i++){
      unique_file_id.emplace(it->second.first);
    }
    mtx_.Lock();
    if(unique_file_id.size() >= leafnodescan_threshold) {
      for(auto file_id_it = unique_file_id.begin(); file_id_it != unique_file_id.end(); file_id_it++){
        FileMetaData* f; //find meta data by sst_id
        f = find_filemeta((*file_id_it),files_);
        if(f){
          candidate_list_ssts[*file_id_it]=f;
        }
        mem_reads_++;
      }
    }
    mtx_.Unlock();
    
    next_key = (it == global_tree.end()) ? global_tree.begin()->first : it->first;
    return next_key;
  }
  
  uint64_t btree_wrapper::findSid(std::string key){
    auto it = global_tree.find(key);
    if (it == global_tree.end()) {
      return -1;
    }else{
      std::pair<uint64_t, uint64_t> entry = it->second;
      return entry.first;
    }
  }

}
