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

using namespace std;

class AtomicInteger {
 public:
  AtomicInteger() {
    this->counter_ = 0;
  }

  AtomicInteger(int initial_value) {
    this->counter_ = initial_value;
  }

  int Get() {
    return this->counter_;
  }

  void Set(long new_value) {
    this->counter_ = new_value;
  }

  long LongValue() {
    return (long)this->counter_;
  }

  int GetAndSet(int new_value) {
    for ( ; ; ) {
      int current = this->counter_;
      if (__sync_bool_compare_and_swap(&this->counter_, current, new_value)) {
        return current;
      }
    }
  }

  bool CompareAndSet(int expect, int update) {
    return __sync_bool_compare_and_swap(&this->counter_, expect, update);
  }

  int GetAndIncrement() {
    for ( ; ; ) {
      int current = this->counter_;
      int next = current + 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return current;
      }
    }
  }

  int GetAndDecrement() {
    for ( ; ; ) {
      int current = this->counter_;
      int next = current - 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return current;
      }
    }
  }

  int GetAndAdd(int delta) {
    for ( ; ; ) {
      int current = this->counter_;
      int next = current + delta;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return current;
      }
    }
  }

  int IncrementAndGet() {
    for ( ; ; ) {
      int current = this->counter_;
      int next = current + 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return next;
      }
    }
  }

  int DecrementAndGet() {
    for ( ; ; ) {
      int current = this->counter_;
      int next = current - 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return next;
      }
    }
  }

  int AddAndGet(int delta) {
    for ( ; ; ) {
      int current = this->counter_;
      int next = current + delta;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return next;
      }
    }
  }

 private:
  volatile int counter_;
};


class AtomicLong {
 public:
  AtomicLong() {
    this->counter_ = 0;
  }

  AtomicLong(long initial_value) {
    this->counter_ = initial_value;
  }

  long Get() {
    return this->counter_;
  }

  void Set(long new_value) {
    this->counter_ = new_value;
  }

  long IntValue() {
    return (int)this->counter_;
  }

  long GetAndSet(long new_value) {
    for ( ; ; ) {
      long current = this->counter_;
      if (__sync_bool_compare_and_swap(&this->counter_, current, new_value)) {
        return current;
      }
    }
  }

  bool CompareAndSet(long expect, long update) {
    return __sync_bool_compare_and_swap(&this->counter_, expect, update);
  }

  long GetAndIncrement() {
    for ( ; ; ) {
      long current = this->counter_;
      long next = current + 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return current;
      }
    }
  }

  long GetAndDecrement() {
    for ( ; ; ) {
      long current = this->counter_;
      long next = current - 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return current;
      }
    }
  }

  long GetAndAdd(long delta) {
    for ( ; ; ) {
      long current = this->counter_;
      long next = current + delta;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return current;
      }
    }
  }

  long IncrementAndGet() {
    for ( ; ; ) {
      long current = this->counter_;
      long next = current + 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return next;
      }
    }
  }

  long DecrementAndGet() {
    for ( ; ; ) {
      long current = this->counter_;
      long next = current - 1;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return next;
      }
    }
  }

  long AddAndGet(long delta) {
    for ( ; ; ) {
      long current = this->counter_;
      long next = current + delta;
      if (__sync_bool_compare_and_swap(&this->counter_, current, next)) {
        return next;
      }
    }
  }

 private:
  volatile long counter_;
};


class AtomicBoolean{
 public:
  AtomicBoolean() {
    this->counter_ = 0;
  }

  AtomicBoolean(bool initial_value) {
    this->counter_ = initial_value ? 1 : 0;
  }

  bool Get() {
    return this->counter_ != 0;
  }

  void Set(bool new_value) {
    this->counter_ = new_value ? 1 : 0;
  }

  bool GetAndSet(bool new_value) {
    int u = new_value ? 1 : 0;
    for (;;) {
      int e = this->counter_ ? 1 : 0;
      if (__sync_bool_compare_and_swap(&this->counter_, e, u)) {
        return e != 0;
      }
    }
  }

  bool CompareAndSet(bool expect, bool update) {
    int e = expect ? 1 : 0;
    int u = update ? 1 : 0;
    return __sync_bool_compare_and_swap(&this->counter_, e, u);
  }

 private:
  volatile int counter_;
};


}  // namespace tubemq


#endif  // TUBEMQ_CLIENT_ATOMIC_DEF_H_

