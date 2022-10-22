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

import io.tidb.bigdata.cdc.craft.CraftEventChunkDecoder;
import io.tidb.bigdata.cdc.craft.CraftParser;
import io.tidb.bigdata.cdc.craft.CraftParserState;
import io.tidb.bigdata.cdc.json.JsonEventChunkDecoder;
import io.tidb.bigdata.cdc.json.JsonParser;
import java.util.Iterator;

public interface EventChunkDecoder extends Iterable<Event[]>, Iterator<Event[]> {

  static EventChunkDecoder create(final byte[] key, final byte[] value, final JsonParser parser) {
    return new JsonEventChunkDecoder(key, value, parser);
  }

  static EventChunkDecoder create(final byte[] value,
      final ParserFactory<CraftParser, CraftParserState> parserFactory) {
    return new CraftEventChunkDecoder(value, parserFactory.createParser());
  }
}
