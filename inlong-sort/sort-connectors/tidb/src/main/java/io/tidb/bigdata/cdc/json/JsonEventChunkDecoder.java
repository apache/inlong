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

package io.tidb.bigdata.cdc.json;

import io.tidb.bigdata.cdc.Event;
import io.tidb.bigdata.cdc.EventChunkDecoder;
import io.tidb.bigdata.cdc.Key;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * TiCDC open protocol event decoder, parse key value pairs into chunk of event instances.
 */
public class JsonEventChunkDecoder implements EventChunkDecoder {

  private static final ThreadLocal<ArrayList<Event>> tlsEventBuffer =
      ThreadLocal.withInitial(ArrayList::new);
  private final Event[] events;
  private final Iterator<Event[]> iterator;

  public JsonEventChunkDecoder(final byte[] key, final byte[] value, final JsonParser parser) {
    events = decode(key, value, parser);
    iterator = iterator();
  }

  private static Event[] decode(final byte[] keyBits, final byte[] valueBits,
      final JsonParser parser) {
    final JsonKeyDecoder keys = new JsonKeyDecoder(keyBits, parser);
    final JsonValueDecoder values = new JsonValueDecoder(valueBits, parser);
    final ArrayList<Event> buffer = tlsEventBuffer.get();
    while (keys.hasNext()) {
      final Key key = keys.next();
      buffer.add(new Event(key, values.next(key)));
    }
    final Event[] events = buffer.toArray(new Event[0]);
    buffer.clear();
    return events;
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
