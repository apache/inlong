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

import io.tidb.bigdata.cdc.Codec;
import io.tidb.bigdata.cdc.EventDecoder;
import io.tidb.bigdata.cdc.KeyDecoder;
import io.tidb.bigdata.cdc.ParserFactory;
import io.tidb.bigdata.cdc.ValueDecoder;
import java.io.Serializable;

public class JsonCodec implements Codec, Serializable {
  private final ParserFactory<JsonParser, JsonNode> parserFactory;

  public JsonCodec() {
    parserFactory = ParserFactory.json();
  }

  public JsonCodec(String shadePrefix) {
    parserFactory = ParserFactory.json(shadePrefix);
  }

  @Override
  public Type type() {
    return Type.JSON;
  }

  @Override
  public EventDecoder decode(byte[] data) {
    throw new IllegalArgumentException("json codec doesn't support single payload format");
  }

  @Override
  public EventDecoder decode(byte[] key, byte[] value) {
    return EventDecoder.json(key, value, parserFactory);
  }

  @Override
  public KeyDecoder key(byte[] key) {
    return KeyDecoder.json(key, parserFactory);
  }

  @Override
  public ValueDecoder value(byte[] value) {
    return ValueDecoder.json(value, parserFactory);
  }
}
