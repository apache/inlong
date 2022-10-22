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

import io.tidb.bigdata.cdc.craft.CraftParser;
import io.tidb.bigdata.cdc.craft.CraftParserState;
import io.tidb.bigdata.cdc.json.JsonNode;
import io.tidb.bigdata.cdc.json.JsonParser;
import io.tidb.bigdata.cdc.json.jackson.JacksonFactory;
import java.io.Serializable;

/*
 * Parser factory interface
 */
public interface ParserFactory<P extends Parser<T>, T> extends Serializable {

  static ParserFactory<JsonParser, JsonNode> json() {
    return JacksonFactory.create();
  }

  static ParserFactory<JsonParser, JsonNode> json(final String shadePrefix) {
    return JacksonFactory.create(shadePrefix);
  }

  static ParserFactory<CraftParser, CraftParserState> craft() {
    return CraftParser::getInstance;
  }

  P createParser();
}
