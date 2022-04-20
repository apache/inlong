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

package org.apache.inlong.manager.common.settings;

public class InlongGroupSettings {

    public static final String DATA_FLOW_GROUP_ID_KEY = "inlong.group.id";

    public static final String DATA_FLOW = "dataFlow";

    /**
     * config of pulsar
     */
    public static final String PULSAR_ADMIN_URL = "pulsar.adminUrl";

    public static final String PULSAR_SERVICE_URL = "pulsar.serviceUrl";

    public static final String PULSAR_AUTHENTICATION = "pulsar.authentication";

    public static final String PULSAR_AUTHENTICATION_TYPE = "pulsar.authentication.type";

    public static final String DEFAULT_PULSAR_AUTHENTICATION_TYPE = "token";

    /**
     * config of tube mq
     */
    public static final String TUBE_MANAGER_URL = "tube.manager.url";

    public static final String TUBE_MASTER_URL = "tube.master.url";

    public static final String TUBE_CLUSTER_ID = "tube.cluster.id";

    /**
     * config of dataproxy
     */
    public static final String CLUSTER_DATA_PROXY = "DATA_PROXY";

    /**
     * config of sort
     */
    public static final String SORT_JOB_ID = "sort.job.id";

    public static final String SORT_TYPE = "sort.type";

    public static final String DEFAULT_SORT_TYPE = "flink";

    public static final String SORT_NAME = "sort.name";

    public static final String SORT_URL = "sort.url";

    public static final String SORT_AUTHENTICATION = "sort.authentication";

    public static final String SORT_AUTHENTICATION_TYPE = "sort.authentication.type";

    public static final String DEFAULT_SORT_AUTHENTICATION_TYPE = "secret_and_token";

    public static final String SORT_PROPERTIES = "sort.properties";

}
