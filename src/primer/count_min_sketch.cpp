//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <cstdint>
#include <stdexcept>
#include <string>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */
  // 1. 检查不合理的参数
  if (width == 0 || depth == 0) {
    throw std::invalid_argument("Width and depth must be greater than 0.");
  }

  // 2. 初始化成员变量
  this->width_ = width;
  this->depth_ = depth;
  count_matrix_ = std::vector<std::vector<uint32_t>>(depth_, std::vector<uint32_t>(width_, 0));

  /** @fall2025 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  // 3. 初始化depth_个哈希函数
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    // 这个哈希函数会将item映射为每行的索引
    hash_functions_.push_back(this->HashFunction(i));
  }
}

// 移动构造函数
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept : width_(other.width_), depth_(other.depth_) {
  
  /** @TODO(student) Implement this function! */
  // 移动 count_matrix_ 的所有权
  this->count_matrix_ = std::move(other.count_matrix_);

  // 确保count_matrix_的每一行长度正确
  // 这里的代码作用是确保在移动构造函数中，count_matrix_的每一行都被调整为当前对象的width_宽度，多余的元素会被截断，不足的会补0。
  // 这样做的目的是防止移动后行宽与新对象的width_不一致，保证数据结构的正确性和一致性。
  for (auto &row : count_matrix_) {
    row.resize(width_, 0);
  }

  // 这里不能直接move other.hash_functions_，而必须重新生成hash_functions_，原因如下：
  // 1. hash_functions_中的每个哈希函数都捕获了当前对象的width_和depth_（通过this指针），
  //    如果直接move过来，lambda中的this仍然指向other对象，导致哈希行为错误甚至未定义行为。
  // 2. 移动后本对象的width_和depth_已变，哈希函数必须基于新对象的参数重新生成，才能保证哈希分布正确。
  // 3. 这样做可以确保CountMinSketch的哈希函数总是与其width_和depth_一致，避免潜在的bug。
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  // 重置 other 的状态
  other.width_ = 0;
  other.depth_ = 0;
}

// 移动赋值运算符
template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  if (this == &other) {
    return *this;
  }

  // 移动资源
  this->width_ = other.width_;
  this->depth_ = other.depth_;
  this->count_matrix_ = std::move(other.count_matrix_);

  // 确保count_matrix_的每一行长度正确
  for (auto &row : count_matrix_) {
    row.resize(width_, 0);
  }

  // 重新生成hash_functions_，因为它们依赖于width_和depth_
  hash_functions_.clear();
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  // 重置 other 的状态
  other.width_ = 0;
  other.depth_ = 0;

  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  // 1. 计算item的索引
  // 2. 将索引对应的计数器加1

  for (uint32_t i = 0; i < depth_; i++) {
    uint32_t index = hash_functions_[i](item);
    // 尽可能降低锁的粒度
    std::lock_guard<std::mutex> lock(mutex_);
    (count_matrix_)[i][index]++;
  }
  // std::lock_guard自动管理互斥锁，无需显式解锁
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  for (uint32_t i = 0; i < depth_; i++) {
    for (uint32_t j = 0; j < width_; j++) {
      (count_matrix_)[i][j] += (other.count_matrix_)[i][j];
    }
  }
  // 其实对于两个二维vector（如std::vector<std::vector<uint32_t>>）的逐元素求和，
  // 由于内存布局是分散的（每一行是独立的vector），最直接的方式就是双重for循环遍历每个元素相加，
  // 这已经是cache友好且高效的做法。可以考虑如下优化建议：
  // 1. 如果每一行的长度很大，可以用多线程并行处理每一行（如OpenMP或std::thread），但对于小矩阵收益不大。
  // 2. 如果数据类型支持SIMD指令（如uint32_t），可以用SIMD批量加速，但C++标准库vector不保证内存对齐，手写难度较高。
  // 3. 如果二维vector是连续内存（如std::vector<uint32_t>模拟二维），可以直接用std::transform或memcpy+循环。
  // 4. 对于本项目这种小型计数矩阵，双重for循环已足够高效。
  // 总结：对于Count-Min Sketch的合并，直接双重for循环是最实用且高效的方式。
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  // 检查数据结构是否有效
  if (count_matrix_.empty() || count_matrix_.size() != depth_ || hash_functions_.size() != depth_) {
    return 0;
  }

  uint32_t min_count = UINT32_MAX;
  for (uint32_t i = 0; i < depth_; i++) {
    // 检查每一行的长度是否正确
    if (count_matrix_[i].size() != width_) {
      // 如果长度不匹配，返回一个合理的默认值
      return count_matrix_[i].empty() ? 0 : count_matrix_[i][0];
    }
    size_t hash_index = hash_functions_[i](item);
    if (hash_index >= width_) {
      // 防止越界访问
      continue;
    }
    min_count = std::min(min_count, count_matrix_[i][hash_index]);
  }
  return min_count == UINT32_MAX ? 0 : min_count;
}

// 清空计数矩阵
// resets the data structure from previous streams
template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  // 只清空计数矩阵的内容，保持维度不变以支持后续的Merge操作
  std::lock_guard<std::mutex> lock(mutex_);
  for (size_t i = 0; i < depth_; ++i) {
    for (size_t j = 0; j < width_; ++j) {
      count_matrix_[i][j] = 0;
    }
  }
  // 不重置width_和depth_，因为这些是结构性参数，应该保持不变
  // 不清空hash_functions_，因为它们在后续操作中还需要使用
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  std::vector<std::pair<KeyType, uint32_t>> top_k;
  if (k == 0) {
    return top_k;
  }
  for (const auto &candidate : candidates) {
    uint32_t count = Count(candidate);
    top_k.emplace_back(candidate, count);
  }

  std::sort(top_k.begin(), top_k.end(), [](const auto &a, const auto &b) {
    return a.second > b.second;  // 降序排序
  });

  // 保留前k个元素
  top_k.resize(std::min(static_cast<size_t>(k), top_k.size()));
  return top_k;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
