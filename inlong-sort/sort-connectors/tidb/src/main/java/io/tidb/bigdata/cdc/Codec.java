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

import io.tidb.bigdata.cdc.craft.CraftCodec;
import io.tidb.bigdata.cdc.json.JsonCodec;

public interface Codec {
  enum Type {
    CRAFT,
    JSON
  }

  static Codec craft() {
    return new CraftCodec();
  }

  static Codec json() {
    return new JsonCodec();
  }

  static Codec json(String shadePrefix) {
    return new JsonCodec(shadePrefix);
  }

  Type type();

  /**
   * Decode single payload events
   * @param data serialized data
   * @return EventDecoder
   */
  EventDecoder decode(byte[] data);

  /**
   * Decode key value paired events
   * @param key serialized key
   * @param value serialized value
   * @return EventDecoder
   */
  EventDecoder decode(byte[] key, byte[] value);

  /**
   * Decode key only
   * @param key serialized key
   * @return KeyDecoder
   */
  KeyDecoder key(byte[] key);

  /**
   * Decode value only
   * @param value serialized value
   * @return ValueDecoder
   */
  ValueDecoder value(byte[] value);
}