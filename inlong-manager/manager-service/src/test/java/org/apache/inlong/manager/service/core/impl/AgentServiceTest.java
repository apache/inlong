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

package org.apache.inlong.manager.service.core.impl;

import com.google.common.collect.Lists;
import org.apache.inlong.common.constant.Constants;
import org.apache.inlong.common.db.CommandEntity;
import org.apache.inlong.common.enums.PullJobTypeEnum;
import org.apache.inlong.common.pojo.agent.DataConfig;
import org.apache.inlong.common.pojo.agent.TaskRequest;
import org.apache.inlong.common.pojo.agent.TaskResult;
import org.apache.inlong.common.pojo.agent.TaskSnapshotMessage;
import org.apache.inlong.common.pojo.agent.TaskSnapshotRequest;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.enums.SourceStatus;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.source.file.FileSourceRequest;
import org.apache.inlong.manager.pojo.source.mysql.MySQLBinlogSourceRequest;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.AgentService;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.Date;

/**
 * Agent service test
 */
class AgentServiceTest extends ServiceBaseTest {

    @Autowired
    private StreamSourceService sourceService;
    @Autowired
    private AgentService agentService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    /**
     * Save source info.
     */
    public Integer saveSource() {
        streamServiceTest.saveInlongStream(GLOBAL_GROUP_ID, GLOBAL_STREAM_ID, GLOBAL_OPERATOR);
        MySQLBinlogSourceRequest sourceInfo = new MySQLBinlogSourceRequest();
        sourceInfo.setInlongGroupId(GLOBAL_GROUP_ID);
        sourceInfo.setInlongStreamId(GLOBAL_STREAM_ID);
        sourceInfo.setSourceType(SourceType.MYSQL_BINLOG);
        sourceInfo.setSourceName("binlog_source_in_agent_service_test");
        return sourceService.save(sourceInfo, GLOBAL_OPERATOR);
    }

    /**
     * Save template source
     */
    public Integer saveTemplateSource() {
        streamServiceTest.saveInlongStream(GLOBAL_GROUP_ID, GLOBAL_STREAM_ID, GLOBAL_OPERATOR);
        FileSourceRequest sourceInfo = new FileSourceRequest();
        sourceInfo.setInlongGroupId(GLOBAL_GROUP_ID);
        sourceInfo.setInlongStreamId(GLOBAL_STREAM_ID);
        sourceInfo.setSourceType(SourceType.FILE);
        sourceInfo.setSourceName("template_source_in_agent_service_test");
        sourceInfo.setInlongClusterName(GLOBAL_CLUSTER_NAME);
        return sourceService.save(sourceInfo, GLOBAL_OPERATOR);
    }

    /**
     * Test report snapshot.
     */
    @Test
    void testReportSnapshot() {
        Integer id = this.saveSource();

        TaskSnapshotRequest request = new TaskSnapshotRequest();
        request.setAgentIp("127.0.0.1");
        request.setReportTime(new Date());

        TaskSnapshotMessage message = new TaskSnapshotMessage();
        message.setJobId(id);
        message.setSnapshot("{\"offset\": 100}");
        request.setSnapshotList(Collections.singletonList(message));

        Boolean result = agentService.reportSnapshot(request);
        Assertions.assertTrue(result);

        sourceService.delete(id, GLOBAL_OPERATOR);
    }

    /**
     * Test sub-source task status manipulation.
     */
    @Test
    void testGetAndReportSubSourceTask() {
        // create template source for cluster agents and approve
        final Integer templateId = this.saveTemplateSource();
        sourceService.updateStatus(GLOBAL_GROUP_ID, GLOBAL_STREAM_ID, SourceStatus.TO_BE_ISSUED_ADD.getCode(),
                GLOBAL_OPERATOR);

        // get sub-source task
        TaskRequest getRequest = new TaskRequest();
        getRequest.setAgentIp("127.0.0.1");
        getRequest.setClusterName(GLOBAL_CLUSTER_NAME);
        getRequest.setPullJobType(PullJobTypeEnum.NEW.getType());
        TaskResult result = agentService.getTaskResult(getRequest);
        Assertions.assertEquals(1, result.getDataConfigs().size());
        DataConfig subSourceTask = result.getDataConfigs().get(0);
        // new sub-source version must be 1
        Assertions.assertEquals(1, subSourceTask.getVersion());
        // sub-source's id must be different from its template source
        Assertions.assertNotEquals(templateId, subSourceTask.getTaskId());
        // operation is to add new task
        Assertions.assertEquals(SourceStatus.BEEN_ISSUED_ADD.getCode() % 100,
                Integer.valueOf(subSourceTask.getOp()));

        // report sub-source status
        CommandEntity reportTask = new CommandEntity();
        reportTask.setTaskId(subSourceTask.getTaskId());
        reportTask.setVersion(subSourceTask.getVersion());
        reportTask.setCommandResult(Constants.RESULT_SUCCESS);
        TaskRequest reportRequest = new TaskRequest();
        reportRequest.setAgentIp("127.0.0.1");
        reportRequest.setCommandInfo(Lists.newArrayList(reportTask));
        agentService.report(reportRequest);

        // check sub-source task status
        StreamSource subSource = sourceService.get(subSourceTask.getTaskId());
        Assertions.assertEquals(SourceStatus.SOURCE_NORMAL.getCode(), subSource.getStatus());

        sourceService.delete(templateId, GLOBAL_OPERATOR);
        sourceService.delete(subSource.getId(), GLOBAL_OPERATOR);
    }

}
