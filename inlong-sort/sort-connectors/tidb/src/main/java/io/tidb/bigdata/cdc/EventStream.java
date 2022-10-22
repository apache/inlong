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

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/*
 * Stateful TiCDC event stream that handles messages deduplication for user
 */
public final class EventStream {

  private final PartitionedStream[] partitionedStreams;

  public EventStream(final int partitions, final int capacity) {
    this.partitionedStreams = IntStream
        .range(0, partitions)
        .mapToObj(p -> new PartitionedStream(capacity))
        .toArray(PartitionedStream[]::new);
  }

  public EventStream() {
    this(1, Integer.MAX_VALUE);
  }

  private PartitionedStream stream(final int idx) {
    return partitionedStreams[idx];
  }

  public int put(final int partition, final Event event) throws InterruptedException {
    return stream(partition).put(event);
  }

  public int put(final int partition, final Event[] events) throws InterruptedException {
    return stream(partition).put(events);
  }

  public Event take(final int partition) throws InterruptedException {
    return stream(partition).take();
  }

  public Event poll(final int partition) {
    return stream(partition).poll();
  }

  public Event poll(final int partition, final long timeout, final TimeUnit unit)
      throws InterruptedException {
    return stream(partition).poll(timeout, unit);
  }
}
