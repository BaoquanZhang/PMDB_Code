//
// Created by Baoquan Zhang on 2/4/20.
//

#ifndef LEVELDB_BTREE_WRAPPER_H
#define LEVELDB_BTREE_WRAPPER_H

#include "btree_map.h"
#include <vector>
#include <atomic>
#include <cmath>

/* btree wrapper function
 * We can wrap interfaces, e.g. adding NVM latency, adding
 * multiple keys, etc., in the corresponding functions
 * */
namespace leveldb {

#define NVM_WRITE_LATENCY_NS 500
#define NVM_READ_LATENCY_NS 200

struct FileMetaData;
class Version;

class btree_wrapper {
 public:
  btree_wrapper()
      : cur_key_(""),
        nvm_read_latency_ns_(NVM_READ_LATENCY_NS),
        nvm_write_latency_ns_(NVM_WRITE_LATENCY_NS){};

  /* Searching for a specific key
   * Params:
   *  key: key to search
   *  sst_offset: A pair of sst id and block offset
   * Return:
   *  true if we find the key
   * */
  bool findKey(std::string key, std::pair<uint64_t, uint64_t>& sst_offset);

  /* Inserting a key into the btree
   * Params:
   *  key : the key to insert
   *  sst_id: the id of sst
   *  block_offset : the offset of block containing the key
   * */
  void insertKey(std::string key, uint64_t sst_id, uint64_t block_offset);

  void insertKeys(std::vector<std::string> keys, std::vector<uint64_t> ssts,
                  std::vector<uint64_t> blocks);

  /* Return the current size of whole index
   * */
  uint64_t size() { return global_tree.size(); };

  void erase(std::string key_to_delete) { global_tree.erase(key_to_delete); }

  std::string scanLeafnode(std::string cur_key, uint64_t num);

  uint64_t findSid(std::string key);

  std::string getCurrentKey() { return cur_key_; }
  void setCurrentKey(std::string key) { cur_key_ = key; }

  uint64_t get_mem_reads() { return mem_reads_; }
  void reset_mem_reads() { mem_reads_ = 0; }
  uint64_t get_mem_writes() { return mem_writes_; }
  void reset_mem_writes() { mem_writes_ = 0; }

 private:
  btree::btree_map<std::string, std::pair<uint64_t, uint64_t>> global_tree;
  std::string cur_key_;
  std::atomic<uint64_t> mem_reads_{0};
  std::atomic<uint64_t> mem_writes_{0};
  uint64_t nvm_write_latency_ns_;
  uint64_t nvm_read_latency_ns_;
};
}

#endif //LEVELDB_BTREE_WRAPPER_H
