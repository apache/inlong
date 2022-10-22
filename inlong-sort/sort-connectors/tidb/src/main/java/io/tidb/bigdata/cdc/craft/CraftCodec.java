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

import io.tidb.bigdata.cdc.Codec;
import io.tidb.bigdata.cdc.EventDecoder;
import io.tidb.bigdata.cdc.KeyDecoder;
import io.tidb.bigdata.cdc.ParserFactory;
import io.tidb.bigdata.cdc.ValueDecoder;
import java.io.Serializable;

public class CraftCodec implements Codec, Serializable {
  private final ParserFactory<CraftParser, CraftParserState> parserFactory;

  public CraftCodec() {
    parserFactory = ParserFactory.craft();
  }

  @Override
  public Type type() {
    return Type.CRAFT;
  }

  @Override
  public EventDecoder decode(byte[] data) {
    return EventDecoder.craft(data, parserFactory);
  }

  @Override
  public EventDecoder decode(byte[] ignored, byte[] data) {
    return decode(data);
  }

  @Override
  public KeyDecoder key(byte[] data) {
    return KeyDecoder.craft(data, parserFactory);
  }

  @Override
  public ValueDecoder value(byte[] data) {
    return ValueDecoder.craft(data, parserFactory);
  }
}
