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

package org.apache.inlong.agent.plugin.task.file;

public class AgentErrMsg {

    public static final String CONFIG_SUCCESS = "SUCCESS";

    // data source config error */
    public static final String DATA_SOURCE_CONFIG_ERROR = "ERROR-0-INLONG_AGENT|10001|ERROR"
            + "|ERROR_DATA_SOURCE_CONFIG|";

    // directory not found error */
    public static final String DIRECTORY_NOT_FOUND_ERROR = "ERROR-0-INLONG_AGENT|11001|WARN"
            + "|WARN_DIRECTORY_NOT_EXIST|";

    // watch directory error */
    public static final String WATCH_DIR_ERROR = "ERROR-0-INLONG_AGENT|11002|ERROR"
            + "|ERROR_WATCH_DIR_ERROR|";

    // file error（not found，rotate）
    public static final String FILE_ERROR = "ERROR-0-INLONG_AGENT|10002|ERROR|ERROR_SOURCE_FILE|";

    // read file error
    public static final String FILE_OP_ERROR = "ERROR-1-INLONG_AGENT|30002|ERROR|ERROR_OPERATE_FILE|";

    // disk full
    public static final String DISK_FULL = "ERROR-1-INLONG_AGENT|30001|FATAL|FATAL_DISK_FULL|";

    // out of memory
    public static final String OOM_ERROR = "ERROR-1-INLONG_AGENT|30001|FATAL|FATAL_OOM_ERROR|";

    // watcher error
    public static final String WATCHER_INVALID = "ERROR-1-INLONG_AGENT|40001|WARN|WARN_INVALID_WATCHER|";

    // could not connect to manager
    public static final String CONNECT_MANAGER_ERROR = "ERROR-1-INLONG_AGENT|30002|ERROR"
            + "|ERROR_CANNOT_CONNECT_TO_MANAGER|";

    // send data to dataProxy failed
    public static final String SEND_TO_BUS_ERROR = "ERROR-1-INLONG_AGENT|30003|ERROR|ERROR_SEND_TO_BUS|";

    // operate bdb error
    public static final String BDB_ERROR = "ERROR-1-INLONG_AGENT|30003|ERROR|BDB_OPERATION_ERROR|";

    // buffer full
    public static final String MSG_BUFFER_FULL = "ERROR-1-INLONG_AGENT|40002|WARN|WARN_MSG_BUFFER_FULL|";

    // found event invalid（task has been delete）
    public static final String FOUND_EVENT_INVALID = "ERROR-1-INLONG_AGENT|30003|ERROR"
            + "|FOUND_EVENT_INVALID|";
}
