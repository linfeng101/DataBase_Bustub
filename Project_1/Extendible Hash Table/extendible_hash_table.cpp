//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

    template <typename K, typename V>
    ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
        // Initialize with one bucket
        dir_.push_back(std::make_shared<Bucket>(bucket_size_, 0));
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::IndexOf(const K& key) -> size_t {
        int mask = (1 << global_depth_) - 1;
        return std::hash<K>()(key) & mask;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
        std::scoped_lock<std::mutex> lock(latch_);
        return GetGlobalDepthInternal();
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
        return global_depth_;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
        std::scoped_lock<std::mutex> lock(latch_);
        return GetLocalDepthInternal(dir_index);
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
        return dir_[dir_index]->GetDepth();
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
        std::scoped_lock<std::mutex> lock(latch_);
        return GetNumBucketsInternal();
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
        return num_buckets_;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Find(const K& key, V& value) -> bool {
        std::scoped_lock<std::mutex> lock(latch_);
        size_t index = IndexOf(key);
        return dir_[index]->Find(key, value);
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Remove(const K& key) -> bool {
        std::scoped_lock<std::mutex> lock(latch_);
        size_t index = IndexOf(key);
        return dir_[index]->Remove(key);
    }

    template <typename K, typename V>
    void ExtendibleHashTable<K, V>::Insert(const K& key, const V& value) {
        std::scoped_lock<std::mutex> lock(latch_);
        while (true) {
            size_t index = IndexOf(key);
            std::shared_ptr<Bucket> target_bucket = dir_[index];

            if (target_bucket->Insert(key, value)) {
                return;  // Insert successful
            }

            // Bucket is full, need to handle split
            if (target_bucket->GetDepth() == global_depth_) {
                // Double the directory size
                size_t old_size = dir_.size();
                dir_.resize(2 * old_size);
                for (size_t i = 0; i < old_size; i++) {
                    dir_[i + old_size] = dir_[i];
                }
                global_depth_++;
            }

            // Split the bucket
            int local_depth = target_bucket->GetDepth();
            auto new_bucket = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
            target_bucket->IncrementDepth();

            // Redistribute directory pointers
            size_t mask = 1 << local_depth;
            size_t directory_index = index & (mask - 1);
            for (size_t i = 0; i < dir_.size(); i++) {
                if ((i & (mask - 1)) == directory_index && dir_[i] == target_bucket) {
                    dir_[i] = ((i & mask) == 0) ? target_bucket : new_bucket;
                }
            }

            // Redistribute items
            auto& items = target_bucket->GetItems();
            auto it = items.begin();
            while (it != items.end()) {
                size_t new_index = IndexOf(it->first);
                if (dir_[new_index] != target_bucket) {
                    new_bucket->Insert(it->first, it->second);
                    it = items.erase(it);
                }
                else {
                    ++it;
                }
            }
            num_buckets_++;
        }
    }

    //===--------------------------------------------------------------------===//
    // Bucket
    //===--------------------------------------------------------------------===//
    template <typename K, typename V>
    ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Bucket::Find(const K& key, V& value) -> bool {
        for (const auto& item : list_) {
            if (item.first == key) {
                value = item.second;
                return true;
            }
        }
        return false;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Bucket::Remove(const K& key) -> bool {
        for (auto it = list_.begin(); it != list_.end(); ++it) {
            if (it->first == key) {
                list_.erase(it);
                return true;
            }
        }
        return false;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Bucket::Insert(const K& key, const V& value) -> bool {
        if (IsFull()) {
            // Check for existing key first
            for (auto& item : list_) {
                if (item.first == key) {
                    item.second = value;  // Update value
                    return true;
                }
            }
            return false;  // Bucket is full
        }

        // Update existing key if it exists
        for (auto& item : list_) {
            if (item.first == key) {
                item.second = value;
                return true;
            }
        }

        // Insert new key-value pair
        list_.emplace_back(key, value);
        return true;
    }

    template class ExtendibleHashTable<page_id_t, Page*>;
    template class ExtendibleHashTable<Page*, std::list<Page*>::iterator>;
    template class ExtendibleHashTable<int, int>;
    // test purpose
    template class ExtendibleHashTable<int, std::string>;
    template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub