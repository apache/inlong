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

package org.apache.inlong.agent.constant;

/**
 * Constants of job fetcher.
 */
public class FetcherConstants {

    public static final String AGENT_FETCHER_INTERVAL = "agent.fetcher.interval";
    public static final int DEFAULT_AGENT_FETCHER_INTERVAL = 60;

    public static final String AGENT_MANAGER_REQUEST_TIMEOUT = "agent.manager.request.timeout";
    // default is 30s
    public static final int DEFAULT_AGENT_MANAGER_REQUEST_TIMEOUT = 30;

    // required config
    public static final String AGENT_MANAGER_ADDR = "agent.manager.addr";

    public static final String AGENT_MANAGER_VIP_HTTP_PREFIX_PATH = "agent.manager.vip.http.prefix.path";
    public static final String DEFAULT_AGENT_MANAGER_VIP_HTTP_PREFIX_PATH = "/inlong/manager/openapi";

    public static final String AGENT_MANAGER_TASK_HTTP_PATH = "agent.manager.task.http.path";
    public static final String DEFAULT_AGENT_MANAGER_EXIST_TASK_HTTP_PATH = "/agent/getExistTaskConfig";
    public static final String AGENT_MANAGER_CONFIG_HTTP_PATH = "agent.manager.config.http.path";
    public static final String DEFAULT_AGENT_MANAGER_CONFIG_HTTP_PATH = "/agent/getConfig";

    public static final String INSTALLER_MANAGER_CONFIG_HTTP_PATH = "installer.manager.config.http.path";
    public static final String DEFAULT_INSTALLER_MANAGER_CONFIG_HTTP_PATH = "/installer/getConfig";

    public static final String AGENT_MANAGER_HEARTBEAT_HTTP_PATH = "heartbeat.http.path";
    public static final String DEFAULT_AGENT_MANAGER_HEARTBEAT_HTTP_PATH = "/heartbeat/report";

    public static final String AGENT_HTTP_APPLICATION_JSON = "application/json";

    public static final int AGENT_HTTP_SUCCESS_CODE = 200;

    public static final String AGENT_MANAGER_RETURN_PARAM_DATA = "data";

    public static final String AGENT_MANAGER_AUTH_SECRET_ID = "agent.manager.auth.secretId";
    public static final String AGENT_MANAGER_AUTH_SECRET_KEY = "agent.manager.auth.secretKey";

    public static final String AGENT_GLOBAL_READER_SOURCE_PERMIT = "agent.global.reader.source.permit";
    public static final int DEFAULT_AGENT_GLOBAL_READER_SOURCE_PERMIT = 16 * 1000 * 1000;

    public static final String AGENT_GLOBAL_READER_QUEUE_PERMIT = "agent.global.reader.queue.permit";
    public static final int DEFAULT_AGENT_GLOBAL_READER_QUEUE_PERMIT = 128 * 1000 * 1000;

    public static final String AGENT_GLOBAL_WRITER_PERMIT = "agent.global.writer.permit";
    public static final int DEFAULT_AGENT_GLOBAL_WRITER_PERMIT = 128 * 1000 * 1000;
}
