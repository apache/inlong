/*
 * Copyright 2020 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.jdbc.impl;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class RoundRobinUrlMapper implements Function<String[], String[]> {

  private final AtomicLong offset = new AtomicLong();

  public String[] apply(final String[] input) {
    int currentOffset = (int) (offset.getAndIncrement() % input.length);
    if (currentOffset == 0) {
      return input;
    }
    String[] result = new String[input.length];
    int right = input.length - currentOffset;
    System.arraycopy(input, currentOffset, result, 0, right);
    System.arraycopy(input, 0, result, right, currentOffset);
    return result;
  }
}