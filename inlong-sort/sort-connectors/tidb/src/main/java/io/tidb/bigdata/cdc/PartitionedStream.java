/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.cdc;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/*
 * PartitionedStream is not thread safe and suppose to be
 * used only within a single dispatching thread for a partition
 */
final class PartitionedStream {

  private final StringBuilder idBuilder = new StringBuilder();
  private final Map<String, Long> objectMaxTs = new HashMap<>();
  private final BlockingQueue<Event> queue;
  private long maxTs = -1;

  protected PartitionedStream(final int capacity) {
    this.queue = capacity == Integer.MAX_VALUE ? new LinkedBlockingQueue<>()
        : new ArrayBlockingQueue<>(capacity);
  }

  public int put(final Event[] events) throws InterruptedException {
    int count = 0;
    for (Event event : events) {
      count += put(event);
    }
    return count;
  }

  public int put(final Event event) throws InterruptedException {
    final long ts = event.getTs();
    if (event.getType() == Key.Type.RESOLVED) {
      if (maxTs < ts) {
        maxTs = ts;
      }
      for (Map.Entry<String, Long> entry : objectMaxTs.entrySet()) {
        if (entry.getValue() < ts) {
          objectMaxTs.put(entry.getKey(), ts);
        }
      }
      return 0;
    }

    final Predicate<Long> test = max -> max != null && ts <= max;
    final String id = objectId(event);
    if (test.test(maxTs) || test.test(objectMaxTs.get(id))) {
      return 0;
    }
    objectMaxTs.put(id, ts);
    queue.put(event);
    return 1;
  }

  private String objectId(final Event event) {
    idBuilder
        .append(Optional.ofNullable(event.getSchema()).orElse(""))
        .append('.')
        .append(Optional.ofNullable(event.getTable()).orElse(""))
        .append('.')
        .append(event.getPartition());
    String id = idBuilder.toString();
    idBuilder.setLength(0);
    return id;
  }

  public Event take() throws InterruptedException {
    return queue.take();
  }

  public Event poll() {
    return queue.poll();
  }

  public Event poll(final long timeout, final TimeUnit unit) throws InterruptedException {
    return queue.poll(timeout, unit);
  }
}
