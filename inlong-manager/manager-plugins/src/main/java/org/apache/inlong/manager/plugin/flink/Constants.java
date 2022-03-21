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

package org.apache.inlong.manager.plugin.flink;

public class Constants {

    public static final String HIVE = "hive";

    public static final String SOURCE_INFO = "source_info";

    public static final String SINK_INFO = "sink_info";

    public static final String PULSAR = "pulsar";

    public static final String TUBEMQ = "tubemq";

    public static final String CLICKHOUSE = "clickhouse";

    public static final String KAFKA = "kafka";

    public static final String ICEBERG = "iceberg";

    public static final String TYPE = "type";

    public static final String ENTRYPOINT_CLASS = "org.apache.inlong.sort.singletenant.flink.Entrance";

    public static final String INLONG = "INLONG_";

    public static final String SORT_JAR = "sort-single-tenant-1.1.0-incubating-SNAPSHOT.jar";

    public static final String SAVEPOINT_DIRECTORY = "file:///flink/savepoints";

    // fetch flink conf
    public static final Integer JOB_MANAGER_PORT = 6123;

    public static final Integer PARALLELISM = 1;

    // flag
    public static final String REMOTE_FORWARD = "Forward";

    public static final String RESOURCE_ID = "resource_id";

    public static final String REMOTE_ENDPOINT = "remoteEndpoint";

    public static final String REMOTE_PARAM = "remoteParam";

    public static final String DATA_PATH = "data_path";

    public static final String COS = "cosn://";

    public static final String CHDFS = "ofs://";

    //REST API URL
    public static final String JOB_URL = "/jobs";

    public static final String SUSPEND_URL = "/stop";

    public static final String JARS_URL = "/jars";

    public static final String UPLOAD = "/upload";

    public static final String RUN_URL = "/run";

    public static final String SAVEPOINT = "/savepoints";

    public static final String HTTP_URL = "http://";

    public static final String URL_SEPARATOR = "/";

    public static final String SEPARATOR = ":";

    public static final boolean DRAIN = false;

}
