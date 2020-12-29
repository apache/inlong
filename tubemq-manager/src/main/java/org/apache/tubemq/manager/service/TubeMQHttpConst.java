/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.manager.service;

public class TubeMQHttpConst {
    public static final String SCHEMA = "http://";
    public static final String BROKER_RUN_STATUS =
            "/webapi.htm?type=op_query&method=admin_query_broker_run_status";
    public static final String TOPIC_CONFIG_INFO =
            "/webapi.htm?type=op_query&method=admin_query_topic_info";
    public static final String ADD_TUBE_TOPIC =
            "/webapi.htm?type=op_modify&method=admin_add_new_topic_record";
    public static final String RELOAD_BROKER =
            "/webapi.htm?type=op_modify&method=admin_reload_broker_configure";
    public static final String QUERY_BROKER_CONFIG = "admin_query_broker_configure";
    public static final String OP_QUERY = "op_query";
    public static final String OP_MODIFY = "op_modify";
    public static final String BATCH_ADD_BROKER = "admin_bath_add_broker_configure";
    public static final String WEB_API = "webapi";
}
