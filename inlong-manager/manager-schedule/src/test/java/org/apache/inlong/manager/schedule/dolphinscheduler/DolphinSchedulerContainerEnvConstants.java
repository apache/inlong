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

package org.apache.inlong.manager.schedule.dolphinscheduler;

public class DolphinSchedulerContainerEnvConstants {

    // DS env image related constants
    protected static final String DS_IMAGE_NAME = "apache/dolphinscheduler-standalone-server";
    protected static final String DS_IMAGE_TAG = "3.2.2";
    protected static final String INTER_CONTAINER_DS_ALIAS = "dolphinscheduler";

    // DS env url related constants
    protected static final String DS_DEFAULT_SERVICE_URL = "http://127.0.0.1:12345/dolphinscheduler";
    protected static final String DS_LOGIN_URL = "/login";
    protected static final String DS_TOKEN_URL = "/access-tokens";
    protected static final String DS_TOKEN_GEN_URL = "/generate";

    // DS env api params related constants
    protected static final String DS_USERNAME = "userName";
    protected static final String DS_PASSWORD = "userPassword";
    protected static final String DS_USERID = "userId";
    protected static final String DS_COOKIE = "Cookie";
    protected static final String DS_COOKIE_SC_TYPE = "securityConfigType";
    protected static final String DS_COOKIE_SESSION_ID = "sessionId";
    protected static final String DS_EXPIRE_TIME = "expireTime";
    protected static final String DS_EXPIRE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    // DS env token related constants
    protected static final String DS_RESPONSE_TOKEN = "token";

    // DS env default admin user info
    protected static final String DS_DEFAULT_USERNAME = "admin";
    protected static final String DS_DEFAULT_PASSWORD = "dolphinscheduler123";
    protected static final Integer DS_DEFAULT_USERID = 1;

}
