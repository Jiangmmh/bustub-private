//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.h
//
// Identification: src/include/primer/count_min_sketch.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"

// CM-Sketch定义在bustub命名空间中
namespace bustub {

template <typename KeyType>
class CountMinSketch {
 public:
  /**
   * @brief 构造一个指定宽度和深度的Count-Min Sketch。
   * @param width  哈希桶的数量（每一行的宽度）
   * @param depth  哈希函数的数量（矩阵的深度/行数）
   *
   * 该构造函数会初始化一个宽为width、深为depth的计数矩阵，并为每一行生成一个独立的哈希函数。
   * Count-Min Sketch是一种概率型数据结构，用于估算数据流中元素出现的频率，能够在空间受限的情况下高效处理大规模数据。
   */
  // explicit关键字用于防止构造函数发生隐式类型转换，避免如CountMinSketch cms = {10,
  // 5};这样的隐式构造，必须显式调用构造函数
  explicit CountMinSketch(uint32_t width, uint32_t depth);

  // 删除默认构造函数、拷贝构造函数和拷贝赋值运算符
  CountMinSketch() = delete;                                            // Default constructor deleted
  CountMinSketch(const CountMinSketch &) = delete;                      // Copy constructor deleted
  auto operator=(const CountMinSketch &) -> CountMinSketch & = delete;  // Copy assignment deleted

  /**
   * @brief 解释noexcept
   *
   * C++中的noexcept关键字用于指明一个函数在运行时不会抛出任何异常。
   * 1. 如果在函数声明或定义后加上noexcept（如：void foo() noexcept;），
   *    则编译器会假定该函数不会抛出异常。如果该函数实际抛出了异常，程序会直接调用std::terminate终止。
   * 2. 使用noexcept有助于编译器进行优化，并且在某些标准库容器移动操作（如std::vector的move）中，
   *    只有当移动构造函数/赋值操作被声明为noexcept时，才会优先使用移动而不是拷贝。
   * 3. 在微服务架构或高性能系统中，noexcept可以提升异常安全性和运行效率。
   *
   * 例如：
   *   CountMinSketch(CountMinSketch &&other) noexcept;
   *   // 表示该移动构造函数不会抛出异常
   */
  // 移动构造函数和移动赋值运算符
  CountMinSketch(CountMinSketch &&other) noexcept;  // Move constructor
  // 这是C++11及以后常用的“尾返回类型”写法，
  // 等价于：CountMinSketch &operator=(CountMinSketch &&other) noexcept;
  // 解释如下：
  // 1. auto 表示编译器自动推导返回类型，但通过 -> CountMinSketch & 显式指定了返回类型为 CountMinSketch 的左值引用。
  // 2. operator=
  // 是移动赋值运算符，用于将另一个右值CountMinSketch对象的资源“移动”到当前对象，避免不必要的拷贝，提高效率。
  // 3. (CountMinSketch &&other) 表示参数是一个右值引用，只能绑定到临时对象或std::move后的对象，实现移动语义。
  // 4. noexcept 表示该函数不会抛出异常，有助于编译器优化，且在标准库容器中优先使用移动操作。
  auto operator=(CountMinSketch &&other) noexcept -> CountMinSketch &;  // 移动赋值运算符，返回自身引用

  /**
   * @brief Inserts an item into the count-min sketch
   *
   * @param item The item to increment the count for
   * @note Updates the min-heap at the same time
   */
  void Insert(const KeyType &item);

  /**
   * @brief Gets the estimated count of an item
   *
   * @param item The item to look up
   * @return The estimated count
   */
  auto Count(const KeyType &item) const -> uint32_t;

  /**
   * @brief Resets the sketch to initial empty state
   *
   * @note Clears the sketch matrix, item set, and top-k min-heap
   */
  void Clear();

  /**
   * @brief Merges the current CountMinSketch with another, updating the current sketch
   * with combined data from both sketches.
   *
   * @param other The other CountMinSketch to merge with.
   * @throws std::invalid_argument if the sketches' dimensions are incompatible.
   */
  void Merge(const CountMinSketch<KeyType> &other);

  /**
   * @brief Gets the top k items based on estimated counts from a list of candidates.
   *
   * @param k Number of top items to return (will be capped at initial k)
   * @param candidates List of candidate items to consider for top k
   * @return Vector of (item, count) pairs in descending count order
   */
  auto TopK(uint16_t k, const std::vector<KeyType> &candidates) -> std::vector<std::pair<KeyType, uint32_t>>;

 private:
  /** Dimensions of the count-min sketch matrix */
  uint32_t width_;  // Number of buckets for each hash function
  uint32_t depth_;  // Number of independent hash functions
  /** Pre-computed hash functions for each row */
  std::vector<std::function<size_t(const KeyType &)>> hash_functions_;

  /** @fall2025 PLEASE DO NOT MODIFY THE FOLLOWING */
  constexpr static size_t SEED_BASE = 15445;

  /**
   * @brief Seeded hash function generator
   *
   * @param seed Used for creating independent hash functions
   * @return A function that maps items to column indices
   */
  inline auto HashFunction(size_t seed) -> std::function<size_t(const KeyType &)> {
    return [seed, this](const KeyType &item) -> size_t {
      auto h1 = std::hash<KeyType>{}(item);
      auto h2 = bustub::HashUtil::CombineHashes(seed, SEED_BASE);
      return bustub::HashUtil::CombineHashes(h1, h2) % width_;
    };
  }

  /** @todo (student) can add their data structures that support count-min sketch operations */
  /**
   * 这里没有使用指针（如uint32_t** 或 std::vector<uint32_t*>）来存储计数矩阵的原因如下：
   * 1. 安全性与易用性：直接使用std::vector<std::vector<uint32_t>>可以自动管理内存，避免手动new/delete带来的内存泄漏和悬垂指针等问题。
   * 2. 简化生命周期管理：vector会自动在对象析构时释放所有资源，无需手动释放，代码更简洁健壮。
   * 3. 支持拷贝/移动语义：vector天然支持拷贝和移动操作，方便CountMinSketch的拷贝构造、移动构造和赋值等操作。
   * 4. 更好的异常安全：vector的操作具有强异常安全保证，异常发生时不会导致内存泄漏。
   * 5. 性能优化：vector的内存布局更有利于CPU缓存友好，遍历和批量操作时性能更高。
   * 6. 代码风格统一：现代C++推荐优先使用智能指针和容器类，减少原始指针的直接使用。
   * 总结：使用vector嵌套vector而非指针，能让代码更安全、易维护、性能更优，是现代C++的最佳实践。
   */
  std::vector<std::vector<uint32_t>> count_matrix_;
  
  // 互斥锁，保护count_matrix_的线程安全
  mutable std::mutex mutex_;
};

}  // namespace bustub
