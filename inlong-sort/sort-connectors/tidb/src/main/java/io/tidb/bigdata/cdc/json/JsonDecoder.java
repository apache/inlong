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

import static io.tidb.bigdata.cdc.Misc.uncheckedRun;

import java.io.DataInputStream;
import java.util.Iterator;

/*
 * Decoder to parse bytes into JsonNode instances
 */
public class JsonDecoder implements Iterator<JsonNode> {

  private static final byte[] EMPTY = new byte[0];
  private final DataInputStream input;
  private final JsonParser parser;

  JsonDecoder(final DataInputStream input, final JsonParser parser) {
    this.input = input;
    this.parser = parser;
  }

  @Override
  public boolean hasNext() {
    return uncheckedRun(() -> input.available() > 0);
  }

  @Override
  public JsonNode next() {
    return uncheckedRun(() -> {
      final long length = input.readLong();
      final byte[] bits;
      if (length == 0) {
        bits = EMPTY;
      } else {
        bits = new byte[(int) length];
        input.readFully(bits);
      }
      return parser.parse(bits);
    });
  }
}
