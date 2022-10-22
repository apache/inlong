/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.cdc.json.jackson;

import java.math.BigDecimal;

public class JacksonObjectNode {

  private final JacksonContext context;
  private final Object object;

  JacksonObjectNode(final JacksonContext context, final Object object) {
    this.context = context;
    this.object = object;
  }

  Object getImpl() {
    return object;
  }

  public JacksonObjectNode put(String fieldName, BigDecimal v) {
    context.objectNodePut(object, fieldName, v);
    return this;
  }

  public JacksonObjectNode put(String fieldName, boolean v) {
    return put(fieldName, Boolean.valueOf(v));
  }

  public JacksonObjectNode put(String fieldName, Boolean v) {
    context.objectNodePut(object, fieldName, v);
    return this;
  }

  public JacksonObjectNode put(String fieldName, byte[] v) {
    context.objectNodePut(object, fieldName, v);
    return this;
  }

  public JacksonObjectNode put(String fieldName, double v) {
    return put(fieldName, Double.valueOf(v));
  }

  public JacksonObjectNode put(String fieldName, Double v) {
    context.objectNodePut(object, fieldName, v);
    return this;
  }

  public JacksonObjectNode put(String fieldName, float v) {
    return put(fieldName, Float.valueOf(v));
  }

  public JacksonObjectNode put(String fieldName, Float v) {
    context.objectNodePut(object, fieldName, v);
    return this;
  }

  public JacksonObjectNode put(String fieldName, short v) {
    return put(fieldName, Short.valueOf(v));
  }

  public JacksonObjectNode put(String fieldName, Short v) {
    context.objectNodePut(object, fieldName, v);
    return this;
  }

  public JacksonObjectNode put(String fieldName, int v) {
    return put(fieldName, Integer.valueOf(v));
  }

  public JacksonObjectNode put(String fieldName, Integer v) {
    context.objectNodePut(object, fieldName, v);
    return this;
  }

  public JacksonObjectNode put(String fieldName, long v) {
    return put(fieldName, Long.valueOf(v));
  }

  public JacksonObjectNode put(String fieldName, Long v) {
    context.objectNodePut(object, fieldName, v);
    return this;
  }

  public JacksonObjectNode put(String fieldName, String v) {
    context.objectNodePut(object, fieldName, v);
    return this;
  }

  public JacksonObjectNode putNull(String fieldName) {
    context.objectNodePutNull(object, fieldName);
    return this;
  }

  public JacksonObjectNode putObject(String fieldName) {
    return new JacksonObjectNode(context, context.objectNodePutObject(object, fieldName));
  }
}

