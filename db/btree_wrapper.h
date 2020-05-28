//
// Created by Baoquan Zhang on 2/4/20.
//

#ifndef LEVELDB_BTREE_WRAPPER_H
#define LEVELDB_BTREE_WRAPPER_H

#include "btree_map.h"
#include <vector>

/* btree wrapper function
 * We can wrap interfaces, e.g. adding NVM latency, adding
 * multiple keys, etc., in the corresponding functions
 * */
namespace leveldb {

  struct FileMetaData;

  class btree_wrapper {
  public:
    btree_wrapper() : cur_key_("") {};

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
    void insertKey(std::string key, uint64_t sst_id, uint64_t block_offset,std::vector<FileMetaData*>** files_);

    void insertKeys(std::vector<std::string> keys,
        std::vector<uint64_t> ssts,
        std::vector<uint64_t> blocks,
        std::vector<FileMetaData*>** files_);

    /* Return the current size of whole index
     * */
    uint64_t size() {
      return global_tree.size();
    };

    std::string scanLeafnode(std::string cur_key, int num,std::vector<FileMetaData*>** files_);

    uint64_t findSid(std::string key);

    std::string getCurrentKey() {return cur_key_; }
    void setCurrentKey(std::string key) { cur_key_ = key; }

  private:
    btree::btree_map<std::string, std::pair<uint64_t, uint64_t>> global_tree;
    std::string cur_key_;
  };
}

#endif //LEVELDB_BTREE_WRAPPER_H
