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

package org.apache.inlong.audit.tool.config;

/**
 * Config constants
 */
public class ConfigConstants {

    public static final String AUDIT_TOOL_NAME = "audit-tool";
    public static final String KEY_GROUP_ID = "group_id";
    public static final String KEY_STREAM_ID = "stream_id";
    public static final String KEY_ALERT_TYPE = "alert_type";
    public static final String KEY_PROMETHEUS_PORT = "audit.store.prometheus.port";
    public static final String KEY_PROMETHEUS = "prometheus";
    public static final String KEY_OTEL = "opentelemetry";
    public static final String KEY_OTEL_ENDPOINT = "otel.exporter.endpoint";
    public static final int DEFAULT_PROMETHEUS_PORT = 10083;
    public static final String DEFAULT_OTEL_ENDPOINT = "http://localhost:4317";

    public static final String AUDIT_TOOL_ALERTS_TOTAL = "audit_tool_alerts_total";
    public static final String DESC_AUDIT_TOOL_ALERTS_TOTAL = "Total number of alerts";
    public static final String AUDIT_TOOL_DATA_LOSS_RATE = "audit_tool_data_loss_rate";
    public static final String DESC_AUDIT_TOOL_DATA_LOSS_RATE = "Data loss rate between Sort and DataProxy";
}
