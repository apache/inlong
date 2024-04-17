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

package org.apache.inlong.audit.config;

/**
 * Open api constants
 */
public class OpenApiConstants {

    // Api config
    public static final String KEY_API_HOUR_PATH = "api.hour.path";
    public static final String DEFAULT_API_HOUR_PATH = "/audit/query/hour";
    public static final String KEY_API_DAY_PATH = "api.day.path";
    public static final String DEFAULT_API_DAY_PATH = "/audit/query/day";
    public static final String KEY_API_MINUTES_PATH = "api.minute.path";
    public static final String DEFAULT_API_MINUTES_PATH = "/audit/query/minutes";
    public static final String KEY_API_GET_IPS_PATH = "api.get.ips.path";
    public static final String DEFAULT_API_GET_IPS_PATH = "/audit/query/getIps";
    public static final String KEY_API_GET_IDS_PATH = "api.get.ids.path";
    public static final String DEFAULT_API_GET_IDS_PATH = "/audit/query/getIds";
    public static final String KEY_API_POOL_SIZE = "api.pool.size";
    public static final int DEFAULT_POOL_SIZE = 10;
    public static final String KEY_API_BACKLOG_SIZE = "api.backlog.size";
    public static final int DEFAULT_API_BACKLOG_SIZE = 100;
    public static final String KEY_API_REAL_LIMITER_QPS = "api.real.limiter.qps";
    public static final double DEFAULT_API_REAL_LIMITER_QPS = 100.0;

    // Cache config
    public static final String KEY_API_CACHE_MAX_SIZE = "api.cache.max.size";
    public static final int DEFAULT_API_CACHE_MAX_SIZE = 50000000;

    public static final String KEY_API_CACHE_EXPIRED_HOURS = "api.cache.expired.hours";
    public static final int DEFAULT_API_CACHE_EXPIRED_HOURS = 12;

    // Http config
    public static final String START_TIME = "startTime";
    public static final String END_TIME = "endTime";
    public static final String AUDIT_ID = "auditId";
    public static final String AUDIT_TAG = "auditTag";
    public static final String INLONG_GROUP_Id = "inlongGroupId";
    public static final String INLONG_STREAM_Id = "inlongStreamId";
    public static final String IP = "ip";
    public static final String AUDIT_CYCLE = "auditCycle";
    public static final String KEY_HTTP_BODY_SUCCESS = "success";
    public static final String KEY_HTTP_BODY_ERR_MSG = "errMsg";
    public static final String KEY_HTTP_BODY_ERR_DATA = "data";
    public static final String KEY_HTTP_HEADER_CONTENT_TYPE = "Content-Type";
    public static final String VALUE_HTTP_HEADER_CONTENT_TYPE = "application/json;charset=utf-8";
    public static final int BIND_PORT = 80;
    public static final int HTTP_RESPOND_CODE = 200;
    public static final String DEFAULT_AUDIT_TAG = "";
}
