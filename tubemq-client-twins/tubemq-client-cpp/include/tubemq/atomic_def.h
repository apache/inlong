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

#ifndef TUBEMQ_CLIENT_ATOMIC_DEF_H_
#define TUBEMQ_CLIENT_ATOMIC_DEF_H_

#include <stdlib.h>

namespace tubemq {


class AtomicInteger {
 public:
  AtomicInteger() { this->counter_ = 0; }

  AtomicInteger(int32_t initial_value) { this->counter_ = initial_value; }

  int32_t Get() const { return this->counter_; }

  void Set(int32_t new_value) { this->counter_ = new_value; }

  int64_t LongValue() const { return (int64_t)this->counter_; }

  int32_t GetAndSet(int32_t new_value) {
    for (;;) {
      int32_t current = this->counter_;
      if (__sync_bool_compare_and_swap(&this->counter_, current, new_value)) {
        return current;
      }
    }
  }

  bool CompareAndSet(int32_t expect, int32_t update) {
    return __sync_bool_compare_and_swap(&this->counter_, expect, update);
  }

  int32_t GetAndIncrement() {
    for (;;) {
      int32_t current = this->counter_;
      int32_t next = current + 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return current;
      }
    }
  }

  int32_t GetAndDecrement() {
    for (;;) {
      int32_t current = this->counter_;
      int32_t next = current - 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return current;
      }
    }
  }

  int32_t GetAndAdd(int32_t delta) {
    for (;;) {
      int32_t current = this->counter_;
      int32_t next = current + delta;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return current;
      }
    }
  }

  int32_t IncrementAndGet() {
    for (;;) {
      int32_t current = this->counter_;
      int32_t next = current + 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return next;
      }
    }
  }

  int32_t DecrementAndGet() {
    for (;;) {
      int32_t current = this->counter_;
      int32_t next = current - 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return next;
      }
    }
  }

  int32_t AddAndGet(int32_t delta) {
    for (;;) {
      int32_t current = this->counter_;
      int32_t next = current + delta;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return next;
      }
    }
  }

 private:
  volatile int32_t counter_;
};

class AtomicLong {
 public:
  AtomicLong() { this->counter_ = 0; }

  AtomicLong(int64_t initial_value) { this->counter_ = initial_value; }

  int64_t Get() const { return this->counter_; }

  void Set(int64_t new_value) { this->counter_ = new_value; }

  int32_t IntValue() const { return (int32_t)this->counter_; }

  int64_t GetAndSet(int64_t new_value) {
    for (;;) {
      int64_t current = this->counter_;
      if (__sync_bool_compare_and_swap(&this->counter_, current, new_value)) {
        return current;
      }
    }
  }

  bool CompareAndSet(int64_t expect, int64_t update) {
    return __sync_bool_compare_and_swap(&this->counter_, expect, update);
  }

  int64_t GetAndIncrement() {
    for (;;) {
      int64_t current = this->counter_;
      int64_t next = current + 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return current;
      }
    }
  }

  int64_t GetAndDecrement() {
    for (;;) {
      int64_t current = this->counter_;
      int64_t next = current - 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return current;
      }
    }
  }

  int64_t GetAndAdd(int64_t delta) {
    for (;;) {
      int64_t current = this->counter_;
      int64_t next = current + delta;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return current;
      }
    }
  }

  int64_t IncrementAndGet() {
    for (;;) {
      int64_t current = this->counter_;
      int64_t next = current + 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return next;
      }
    }
  }

  int64_t DecrementAndGet() {
    for (;;) {
      int64_t current = this->counter_;
      int64_t next = current - 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return next;
      }
    }
  }

  int64_t AddAndGet(int64_t delta) {
    for (;;) {
      int64_t current = this->counter_;
      int64_t next = current + delta;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return next;
      }
    }
  }

 private:
  volatile int64_t counter_;
};

class AtomicBoolean {
 public:
  AtomicBoolean() { this->counter_ = 0; }

  AtomicBoolean(bool initial_value) { this->counter_ = initial_value ? 1 : 0; }

  bool Get() const { return this->counter_ != 0; }

  void Set(bool new_value) { this->counter_ = new_value ? 1 : 0; }

  bool GetAndSet(bool new_value) {
    int32_t u = new_value ? 1 : 0;
    for (;;) {
      int32_t e = this->counter_ ? 1 : 0;
      if (__sync_bool_compare_and_swap(&this->counter_, e, u)) {
        return e != 0;
      }
    }
  }

  bool CompareAndSet(bool expect, bool update) {
    int32_t e = expect ? 1 : 0;
    int32_t u = update ? 1 : 0;
    return __sync_bool_compare_and_swap(&this->counter_, e, u);
  }

 private:
  volatile int32_t counter_;
};

}  // namespace tubemq

#endif  // TUBEMQ_CLIENT_ATOMIC_DEF_H_
