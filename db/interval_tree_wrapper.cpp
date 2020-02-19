//
// Created by Baoquan Zhang on 2.18
//
#include "interval_tree_wrapper.h"

void interval_tree_wrapper::add_interval(
    const std::string& start, const std::string& end, uint64_t sst, uint64_t offset, uint64_t block_size) {
  std::pair<uint64_t, std::pair<uint64_t, uint64_t>> sst_offset_size =
                                     std::make_pair(sst, std::make_pair(offset, block_size));
  intervals.insert({start, end, sst_offset_size});
}


std::vector<std::pair<uint64_t, std::pair<uint64_t, uint64_t>>> interval_tree_wrapper::find_overlap(
    const std::string& start, const std::string& end) {
  std::vector<std::pair<uint64_t, std::pair<uint64_t, uint64_t>>> targets;
  auto overlapped_intervals = intervals.findOverlappingIntervals({start, end});
  for (const auto& overlapped_interval : overlapped_intervals) {
    targets.push_back(overlapped_interval.value);
  }
  // std::cout << "Target Size: " << targets.size() << std::endl;
  return targets;
}

void interval_tree_wrapper::clear() {
  intervals.clear();
}