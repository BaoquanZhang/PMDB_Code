//
// Created by Baoquan Zhang on 2/17/20.
//

#ifndef LEVELDB_INTERVAL_TREE_WRAPPER_H
#define LEVELDB_INTERVAL_TREE_WRAPPER_H

#include "intervaltree.h"

#include <iostream>
#include <vector>
#include <mutex>
#include <map>
#include <unordered_set>

#include "port/thread_annotations.h"

struct interval {
  interval(std::string start, std::string end) {
    start_ = start;
    end_ = end;
  }
  std::string start_;
  std::string end_;
};

struct target {
  target(uint64_t file_id, uint64_t block_offset, uint64_t block_size) {
    file_id_ = file_id;
    block_offset_ = block_offset;
    block_size_ = block_size;
  }
  target(){
    file_id_ = 0;
    block_offset_ = 0;
    block_size_ = 0;
  }
  bool operator==(const target& a) const {
    return file_id_ == a.file_id_
            && block_size_ == a.block_size_
            && block_offset_ == a.block_offset_;
  }

  bool operator<(const target& a) const {
    return file_id_ < a.file_id_;
  }
  uint64_t file_id_;
  uint64_t block_offset_;
  uint64_t block_size_;
};

class interval_tree_wrapper {
public:
  interval_tree_wrapper() : max_overlap_count(0), is_split_(false) {}
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
  std::vector<target> find_overlap(
      const std::string& start, const std::string& end);

  std::vector<target> find_point(const std::string& key);

  /*
   * Clear all intervals
   * It may be called after a compaction
   * */
  void clear();

  /*
   * Get the max overlapped count
   * */
  uint64_t get_max_overlap_count() { return max_overlap_count; }

  /*
   * Lock/unlock the tree
   * */
  void lock();
  void unlock();

  /*
   * Delete intervals by file id
   * @paras:
   *  files: file ids to delete
   * @return: the number of deleted intervals
   * */
  uint64_t delete_by_file(const std::unordered_set<uint64_t>& files);

  /*
   * Display all intervals
   * */
  void display_intervals();

  /*
   * increase /reset overlap count
   * */
  void increase_overlap() {
    max_overlap_count++;
  }

  void reset_overlap() {
    max_overlap_count = 0;
  }

  /*
   * get all files
   * */
  std::unordered_set<uint64_t> get_files();

  /*
   * get the number of files
   * */
  uint64_t get_file_count() {
    return files.size();
  }

  /*
   * set/get split
   * */
  void set_split(bool is_split) {
    is_split_ = is_split;
  }
  bool is_split() {
    return is_split_;
  }

  /* add files to delete */

  void add_files_to_delete(const std::unordered_set<uint64_t>& files_to_delete) {
    files_to_delete_.insert(files_to_delete.begin(), files_to_delete.end());
  }

  void do_delete_files() {
    delete_by_file(files_to_delete_);
    files_to_delete_.clear();
  }

private:
  //Intervals::IntervalTree<std::string, std::pair<uint64_t, std::pair<uint64_t, uint64_t>>> intervals;
  struct intervalComparator {
    bool operator()(const interval& a, const interval& b) const {
      if (a.start_ == b.start_)
        return a.end_ < b.end_;
      return a.start_ < b.start_;
    }
  };
  Intervals::IntervalTree<std::string, target> intervals;
  //std::map<interval, target, intervalComparator> intervals;
  uint64_t max_overlap_count;
  std::unordered_set<uint64_t> files;
  std::mutex mutex_;
  bool is_split_;
  std::unordered_set<uint64_t> files_to_delete_;
};

#endif //LEVELDB_INTERVAL_TREE_WRAPPER_H
