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

package org.apache.inlong.agent.plugin.task.filecollect;

public class AgentErrMsg {

    public static final String CONFIG_SUCCESS = "SUCCESS";

    // 数据源配置异常 */
    public static final String DATA_SOURCE_CONFIG_ERROR = "ERROR-0-TDAgent|10001|ERROR"
            + "|ERROR_DATA_SOURCE_CONFIG|";

    // 监控文件夹不存在 */
    public static final String DIRECTORY_NOT_FOUND_ERROR = "ERROR-0-TDAgent|11001|WARN"
            + "|WARN_DIRECTORY_NOT_EXIST|";

    // 监控文件夹时出错 */
    public static final String WATCH_DIR_ERROR = "ERROR-0-TDAgent|11002|ERROR"
            + "|ERROR_WATCH_DIR_ERROR|";

    // 要读取的文件异常（不存在，rotate）
    public static final String FILE_ERROR = "ERROR-0-TDAgent|10002|ERROR|ERROR_SOURCE_FILE|";

    // 读取文件异常
    public static final String FILE_OP_ERROR = "ERROR-1-TDAgent|30002|ERROR|ERROR_OPERATE_FILE|";

    // 磁盘满
    public static final String DISK_FULL = "ERROR-1-TDAgent|30001|FATAL|FATAL_DISK_FULL|";

    // 内存溢出
    public static final String OOM_ERROR = "ERROR-1-TDAgent|30001|FATAL|FATAL_OOM_ERROR|";

    // watcher异常
    public static final String WATCHER_INVALID = "ERROR-1-TDAgent|40001|WARN|WARN_INVALID_WATCHER|";

    // 连不上tdmanager
    public static final String CONNECT_TDM_ERROR = "ERROR-1-TDAgent|30002|ERROR"
            + "|ERROR_CANNOT_CONNECT_TO_TDM|";

    // 发送数据到tdbus失败
    public static final String SEND_TO_BUS_ERROR = "ERROR-1-TDAgent|30003|ERROR|ERROR_SEND_TO_BUS|";

    // 操作bdb异常
    public static final String BDB_ERROR = "ERROR-1-TDAgent|30003|ERROR|BDB_OPERATION_ERROR|";

    // 内部缓存满
    public static final String MSG_BUFFER_FULL = "ERROR-1-TDAgent|40002|WARN|WARN_MSG_BUFFER_FULL|";

    // 监控到的事件不合法（任务已删除）
    public static final String FOUND_EVENT_INVALID = "ERROR-1-TDAgent|30003|ERROR"
            + "|FOUND_EVENT_INVALID|";
}
