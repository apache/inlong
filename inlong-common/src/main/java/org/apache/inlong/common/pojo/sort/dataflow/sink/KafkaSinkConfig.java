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

package org.apache.inlong.common.pojo.sort.dataflow.sink;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class KafkaSinkConfig extends SinkConfig {

    public static final String MESSAGE_TYPE_CSV = "csv";
    public static final Character CSV_DEFAULT_DELIMITER = '|';
    public static final String MESSAGE_TYPE_KV = "kv";
    public static final Character KV_DEFAULT_ENTRYSPLITTER = '&';
    public static final Character KV_DEFAULT_KVSPLITTER = '=';
    public static final String MESSAGE_TYPE_JSON = "json";
    private String topicName;
    private String messageType;
    private Character delimiter;
    private Character escapeChar;
    private Character entrySplitter;
    private Character kvSplitter;
}
