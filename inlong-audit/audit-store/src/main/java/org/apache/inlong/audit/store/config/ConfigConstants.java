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

package org.apache.inlong.audit.store.config;

/**
 * Config constants
 */
public class ConfigConstants {

    public static final String AUDIT_STORE_SERVER_NAME = "audit-store";
    public static final String KEY_PROMETHEUS_PORT = "audit.store.prometheus.port";
    public static final int DEFAULT_PROMETHEUS_PORT = 10083;
    public static final String KEY_STORE_METRIC_CLASSNAME = "audit.store.metric.classname";
    public static final String DEFAULT_STORE_METRIC_CLASSNAME =
            "org.apache.inlong.audit.store.metric.prometheus.StorePrometheusMetric";

    public static final String KEY_AUDIT_SERVICE_ADDR = "audit.service.addr";
    public static final String DEFAULT_AUDIT_SERVICE_ADDR = "http://localhost:10080";
    public static final String KEY_AUDIT_SERVICE_TIMEOUT_MS = "audit.service.timeout.ms";
    public static final int DEFAULT_AUDIT_SERVICE_TIMEOUT_MS = 30000;
    public static final String KEY_AUDIT_SERVICE_ROUTE_API = "audit.service.route.api";
    public static final String DEFAULT_AUDIT_SERVICE_ROUTE_API = "/audit/query/getAuditRoute";

    public static final String KEY_AUDIT_STORE_ROUTE_ENABLED = "audit.store.route.enabled";
    public static final boolean DEFAULT_AUDIT_STORE_ROUTE_ENABLED = false;
}
