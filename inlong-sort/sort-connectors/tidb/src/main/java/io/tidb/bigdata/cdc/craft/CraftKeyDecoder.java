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

import io.tidb.bigdata.cdc.Key;
import io.tidb.bigdata.cdc.KeyDecoder;

public class CraftKeyDecoder implements KeyDecoder {

  private final CraftEventDecoder decoder;

  public CraftKeyDecoder(byte[] value, CraftParser parser) {
    decoder = new CraftEventDecoder(value, parser);
  }

  @Override
  public boolean hasNext() {
    return decoder.hasNext();
  }

  @Override
  public Key next() {
    return decoder.next().getKey();
  }
}
