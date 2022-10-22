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

package io.tidb.bigdata.cdc.craft;

import io.tidb.bigdata.cdc.Event;
import io.tidb.bigdata.cdc.EventChunkDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class CraftEventChunkDecoder implements EventChunkDecoder {

  private final Event[] events;
  private final Iterator<Event[]> iterator;

  public CraftEventChunkDecoder(final byte[] value, final CraftParser parser) {
    CraftParserState state = parser.parse(value);
    ArrayList<Event> events = new ArrayList<>();
    while (state.hasNext()) {
      events.add(state.next());
    }
    this.events = events.toArray(new Event[0]);
    this.iterator = iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Event[] next() {
    return iterator.next();
  }

  @Override
  public Iterator<Event[]> iterator() {
    return Collections.singletonList(events).iterator();
  }
}