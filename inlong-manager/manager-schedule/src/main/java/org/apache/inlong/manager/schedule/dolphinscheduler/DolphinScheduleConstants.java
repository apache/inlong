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

public class DolphinScheduleConstants {

    // DS public constants
    public static final String DS_ID = "id";
    public static final String DS_CODE = "code";
    public static final String DS_TOKEN = "token";
    public static final String DS_PAGE_SIZE = "pageSize";
    public static final String DS_PAGE_NO = "pageNo";
    public static final String DS_SEARCH_VAL = "searchVal";
    public static final String DS_RESPONSE_DATA = "data";
    public static final String DS_RESPONSE_NAME = "name";
    public static final String DS_RESPONSE_TOTAL_LIST = "totalList";
    public static final String DS_DEFAULT_PAGE_SIZE = "10";
    public static final String DS_DEFAULT_PAGE_NO = "1";
    public static final String DS_DEFAULT_TIMEZONE_ID = "Asia/Shanghai";

    // DS project related constants
    public static final String DS_PROJECT_URL = "/projects";
    public static final String DS_PROJECT_NAME = "projectName";
    public static final String DS_PROJECT_DESC = "description";
    public static final String DS_DEFAULT_PROJECT_NAME = "default_inlong_offline_scheduler";
    public static final String DS_DEFAULT_PROJECT_DESC = "default scheduler project for inlong offline job";

    // DS task related constants
    public static final String DS_TASK_CODE_URL = "/task-definition/gen-task-codes";
    public static final String DS_TASK_RELATION = "taskRelationJson";
    public static final String DS_TASK_DEFINITION = "taskDefinitionJson";
    public static final String DS_TASK_GEN_NUM = "genNum";
    public static final String DS_DEFAULT_TASK_GEN_NUM = "1";
    public static final String DS_DEFAULT_TASK_NAME = "default-inlong-http-callback";
    public static final String DS_DEFAULT_TASK_DESC = "default http request using shell script callbacks to inlong";

    // DS process definition related constants
    public static final String DS_PROCESS_URL = "/process-definition";
    public static final String DS_PROCESS_QUERY_URL = "/query-process-definition-list";
    public static final String DS_PROCESS_NAME = "name";
    public static final String DS_PROCESS_DESC = "description";
    public static final String DS_PROCESS_CODE = "processDefinitionCode";
    public static final String DS_DEFAULT_PROCESS_NAME = "_inlong_offline_process_definition";
    public static final String DS_DEFAULT_PROCESS_DESC = "scheduler process definition for inlong group: ";

    // DS release related constants
    public static final String DS_RELEASE_URL = "/release";
    public static final String DS_RELEASE_STATE = "releaseState";

    // DS schedule related constants
    public static final String DS_SCHEDULE_URL = "/schedules";
    public static final String DS_SCHEDULE_DEF = "schedule";
    public static final String DS_DEFAULT_SCHEDULE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    // DS online/offline related constants
    public static final String DS_ONLINE_URL = "/online";
    public static final String DS_ONLINE_STATE = "ONLINE";
    public static final String DS_OFFLINE_URL = "/offline";
    public static final String DS_OFFLINE_STATE = "OFFLINE";

}
