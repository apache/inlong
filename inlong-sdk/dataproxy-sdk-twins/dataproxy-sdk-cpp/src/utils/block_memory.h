/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef INLONG_SDK_BLOCK_MEMORY_H
#define INLONG_SDK_BLOCK_MEMORY_H

#include "memory.h"
#include "noncopyable.h"
#include <memory>
#include <string>
namespace inlong {

class BlockMemory : noncopyable {
public:
  explicit BlockMemory(const uint64_t size = 32 * 1024,
                       const std::string &id = "")
      : m_data(new char[size]), m_len(0), m_max_size(size), id_(id) {}

  ~BlockMemory() {
    if (m_data) {
      delete[] m_data;
    }
  };

  void CopyFrom(const BlockMemory &other);

  uint64_t GetFree();

public:
  char *m_data;
  uint64_t m_len;
  const uint64_t m_max_size;
  const std::string id_;
};
typedef std::shared_ptr<BlockMemory> BlockMemoryPtrT;

inline void BlockMemory::CopyFrom(const BlockMemory &other) {
  if (this == &other) {
    return;
  }

  if (m_max_size >= other.m_len) {
    memcpy(m_data, other.m_data, static_cast<size_t>(other.m_len));
    m_len = other.m_len;
  }
}

inline uint64_t BlockMemory::GetFree() { return (m_max_size - m_len); }
} // namespace inlong
#endif // INLONG_SDK_BLOCK_MEMORY_H
