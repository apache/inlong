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

package io.tidb.bigdata.cdc.json.jackson;

import io.tidb.bigdata.cdc.ParserFactory;
import io.tidb.bigdata.cdc.json.JsonNode;
import io.tidb.bigdata.cdc.json.JsonParser;

public class JacksonFactory implements ParserFactory<JsonParser, JsonNode>, JsonParser {

  private final JacksonContext context;
  private final Object reader;
  private final Object writer;

  private JacksonFactory(final JacksonContext context) {
    this.context = context;
    final Object mapper = context.newMapper();
    this.reader = context.newReader(mapper);
    this.writer = context.newWriter(mapper);
  }

  public static JacksonFactory create() {
    return new JacksonFactory(JacksonContext.getDefaultContext());
  }

  public static JacksonFactory create(String shadePrefix) {
    return new JacksonFactory(new JacksonContext(shadePrefix));
  }

  public JsonParser createParser() {
    return this;
  }

  public JacksonObjectNode createObject() {
    return new JacksonObjectNode(context, context.newObject());
  }

  @Override
  public JsonNode parse(byte[] input) {
    if (input.length == 0) {
      return JacksonMissingNode.getInstance();
    }
    return new JacksonJsonNode(context, context.readTree(reader, input));
  }

  public String toJson(JacksonObjectNode object) {
    return context.writeValueAsString(writer, object.getImpl());
  }
}