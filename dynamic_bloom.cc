// Copyright (c) 2013, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "dynamic_bloom.h"

#include <algorithm>

#include "port/port.h"
#include "rocksdb/slice.h"
#include "util/hash.h"

namespace rocksdb {

namespace {
static uint32_t BloomHash(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}

uint32_t GetTotalBitsForLocality(uint32_t total_bits) {
  uint32_t num_blocks =
      (total_bits + CACHE_LINE_SIZE * 8 - 1) / (CACHE_LINE_SIZE * 8);

  // Make num_blocks an odd number to make sure more bits are involved
  // when determining which block.
  if (num_blocks % 2 == 0) {
    num_blocks++;
  }

  return num_blocks * (CACHE_LINE_SIZE * 8);
}
}

DynamicBloom::DynamicBloom(uint32_t total_bits, uint32_t locality,
                           uint32_t num_probes,
                           uint32_t (*hash_func)(const Slice& key),
                           size_t huge_page_tlb_size, Logger* logger)
    : DynamicBloom(num_probes, hash_func) {
  SetTotalBits(total_bits, locality, huge_page_tlb_size, logger);
}

DynamicBloom::DynamicBloom(uint32_t num_probes,
                           uint32_t (*hash_func)(const Slice& key))
    : kTotalBits(0),
      kNumBlocks(0),
      kNumProbes(num_probes),
      hash_func_(hash_func == nullptr ? &BloomHash : hash_func) {}

void DynamicBloom::SetTotalBits(uint32_t total_bits, uint32_t locality,
                                size_t huge_page_tlb_size, Logger* logger) {
  kTotalBits = (locality > 0) ? GetTotalBitsForLocality(total_bits)
                              : (total_bits + 7) / 8 * 8;
  kNumBlocks = (locality > 0) ? (kTotalBits / (CACHE_LINE_SIZE * 8)) : 0;

  assert(kNumBlocks > 0 || kTotalBits > 0);
  assert(kNumProbes > 0);

  uint32_t sz = kTotalBits / 8;
  if (kNumBlocks > 0) {
    sz += CACHE_LINE_SIZE - 1;
  }
  raw_ = reinterpret_cast<unsigned char*>(
      arena_.AllocateAligned(sz, huge_page_tlb_size, logger));
  memset(raw_, 0, sz);
  if (kNumBlocks > 0 && (reinterpret_cast<uint64_t>(raw_) % CACHE_LINE_SIZE)) {
    data_ = raw_ + CACHE_LINE_SIZE -
            reinterpret_cast<uint64_t>(raw_) % CACHE_LINE_SIZE;
  } else {
    data_ = raw_;
  }
}

}  // rocksdb
