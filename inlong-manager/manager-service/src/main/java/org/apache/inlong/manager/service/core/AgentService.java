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

package org.apache.inlong.manager.service.core;

import org.apache.inlong.common.pojo.agent.TaskRequest;
import org.apache.inlong.common.pojo.agent.TaskResult;
import org.apache.inlong.common.pojo.agent.TaskSnapshotRequest;
import org.apache.inlong.manager.common.pojo.agent.AgentStatusReportRequest;
import org.apache.inlong.manager.common.pojo.agent.CheckAgentTaskConfRequest;
import org.apache.inlong.manager.common.pojo.agent.ConfirmAgentIpRequest;
import org.apache.inlong.manager.common.pojo.agent.FileAgentCommandInfo;
import org.apache.inlong.manager.common.pojo.agent.FileAgentTaskConfig;
import org.apache.inlong.manager.common.pojo.agent.FileAgentTaskInfo;

import java.util.List;

/**
 * The service interface for agent
 */
public interface AgentService {

    /**
     * Report the heartbeat for given source.
     *
     * @param request Heartbeat request.
     * @return Whether succeed.
     */
    Boolean reportSnapshot(TaskSnapshotRequest request);

    /**
     * Agent report the task result.
     *
     * @param request Request of the task result.
     * @return Task result.
     */
    void report(TaskRequest request);

    /**
     *  Pull task config to operate.
     *
     * @param request Request of the task result.
     * @return Task result.
     */
    TaskResult getTaskResult(TaskRequest request);

    @Deprecated
    FileAgentTaskInfo getFileAgentTask(FileAgentCommandInfo info);

    String confirmAgentIp(ConfirmAgentIpRequest request);

    List<FileAgentTaskConfig> checkAgentTaskConf(CheckAgentTaskConfRequest request);

    String reportAgentStatus(AgentStatusReportRequest request);

}
