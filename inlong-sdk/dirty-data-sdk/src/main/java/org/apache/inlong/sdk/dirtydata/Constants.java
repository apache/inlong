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

package org.apache.inlong.sdk.dirtydata;

/**
 * Connector base option constant
 */
public final class Constants {

    // The action to deal with schema update in multiple sink.
    public static final String SINK_MULTIPLE_SCHEMA_UPDATE_POLICY = "sink.multiple.schema-update.policy";

    public static final String DIRTY_COLLECT_ENABLE = "dirty.collect.enable";

    public static final String DIRTY_SIDE_OUTPUT_CONNECTOR = "dirty.side-output.connector";

    public static final String DIRTY_SIDE_OUTPUT_IGNORE_ERRORS = "dirty.side-output.ignore-errors";

    /**
     * The labels of dirty side-output, format is 'key1=value1&key2=value2'
     * it supports variable replace like '${variable}'
     * There are two system variables[SYSTEM_TIME|DIRTY_TYPE|DIRTY_MESSAGE]
     * are currently supported,
     * and the support of other variables is determined by the connector.
     */
    public static final String DIRTY_SIDE_OUTPUT_LABELS = "dirty.side-output.labels";

    /**
     * The log tag of dirty side-output, it supports variable replace like '${variable}'.
     * There are two system variables[SYSTEM_TIME|DIRTY_TYPE|DIRTY_MESSAGE] are currently supported,
     * and the support of other variables is determined by the connector.
     */
    public static final String DIRTY_SIDE_OUTPUT_LOG_TAG = "dirty.side-output.log-tag";

    /**
     * It is used for 'inlong.metric.labels' or 'sink.dirty.labels'
     */
    public static final String DELIMITER = "&";

    /**
     * The delimiter of key and value, it is used for 'inlong.metric.labels' or 'sink.dirty.labels'
     */
    public static final String KEY_VALUE_DELIMITER = "=";
}
