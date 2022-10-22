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

import io.tidb.bigdata.cdc.Key;
import io.tidb.bigdata.cdc.KeyDecoder;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

/**
 * TiCDC open protocol json format event key decoder, parse bits into event key instances.
 */
public class JsonKeyDecoder implements KeyDecoder {

  private static final long CURRENT_VERSION = 1;
  private final JsonDecoder decoder;

  public JsonKeyDecoder(final DataInputStream input, final JsonParser parser) {
    if (uncheckedRun(() -> input.readLong() != CURRENT_VERSION)) {
      throw new RuntimeException("Illegal version, should be 1");
    }
    // we know there is only one json parser at this time
    decoder = new JsonDecoder(input, parser);
  }

  public JsonKeyDecoder(final byte[] input, final JsonParser parser) {
    this(new DataInputStream(new ByteArrayInputStream(input)), parser);
  }

  @Override
  public boolean hasNext() {
    return decoder.hasNext();
  }

  @Override
  public Key next() {
    final JsonNode node = decoder.next();
    return new Key(
        node.getText("scm", null),
        node.getText("tbl", null),
        node.getInt("ptn", 0),
        node.mustGetInt("t"),
        node.mustGetLong("ts"));
  }
}
