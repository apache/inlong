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

package org.apache.inlong.manager.schedule.airflow;

/**
 * Contains constants for interacting with the Airflow API.
 */
public class AirFlowAPIConstant {

    public static final String DEFAULT_TIMEZONE = "Asia/Shanghai";
    public static final String INLONG_OFFLINE_DAG_TASK_PREFIX = "inlong_offline_task_";
    public static final String SUBMIT_OFFLINE_JOB_URI = "/inlong/manager/api/group/submitOfflineJob";

    // AirflowConnection
    public static final String LIST_CONNECTIONS_URI = "/api/v1/connections";
    public static final String GET_CONNECTION_URI = "/api/v1/connections/{connection_id}";

    // DAG
    public static final String LIST_DAGS_URI = "/api/v1/dags";
    public static final String UPDATE_DAG_URI = "/api/v1/dags/{dag_id}";

    // DAGRun
    public static final String TRIGGER_NEW_DAG_RUN_URI = "/api/v1/dags/{dag_id}/dagRuns";

}
