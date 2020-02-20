//
// Created by Baoquan Zhang on 2/17/20.
//

#ifndef LEVELDB_INTERVAL_TREE_WRAPPER_H
#define LEVELDB_INTERVAL_TREE_WRAPPER_H

#include "intervaltree.h"

#include <iostream>
#include <vector>

class interval_tree_wrapper {
public:
  /*
   * Add an interval to the interval tree
   * @paras:
   *  start: the start key of the interval
   *  end: the end key of the interval (end key is included in the interval)
   *  target: the target of the interval
   * */
  void add_interval(
      const std::string& start,
      const std::string& end,
      uint64_t sst,
      uint64_t offset,
      uint64_t block_size);

  /*
   * Find all of the targets overlapped interval with [start, end]
   * @Return:
   *  A vector includes all targets
   * */
  std::vector<std::pair<uint64_t, std::pair<uint64_t, uint64_t>>> find_overlap(
      const std::string& start, const std::string& end);

  /*
   * Clear all intervals
   * It may be called after a compaction
   * */
  void clear();

private:
  Intervals::IntervalTree<std::string, std::pair<uint64_t, std::pair<uint64_t, uint64_t>>> intervals;
};

#endif //LEVELDB_INTERVAL_TREE_WRAPPER_H
