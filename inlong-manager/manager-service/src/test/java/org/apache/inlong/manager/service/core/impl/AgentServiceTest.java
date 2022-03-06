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

import org.apache.inlong.common.pojo.agent.TaskSnapshotMessage;
import org.apache.inlong.common.pojo.agent.TaskSnapshotRequest;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceRequest;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.AgentService;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.Date;

/**
 * Stream source service test
 */
public class AgentServiceTest extends ServiceBaseTest {

    @Autowired
    private StreamSourceService sourceService;
    @Autowired
    private AgentService agentService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    public Integer saveSource() {
        streamServiceTest.saveInlongStream(globalGroupId, globalStreamId, globalOperator);

        BinlogSourceRequest sourceInfo = new BinlogSourceRequest();
        sourceInfo.setInlongGroupId(globalGroupId);
        sourceInfo.setInlongStreamId(globalStreamId);
        sourceInfo.setSourceType(Constant.SOURCE_BINLOG);
        sourceInfo.setSourceName(globalStreamName);
        return sourceService.save(sourceInfo, globalOperator);
    }

    @Test
    public void testReportSnapshot() {
        Integer id = this.saveSource();

        TaskSnapshotRequest request = new TaskSnapshotRequest();
        request.setAgentIp("127.0.0.1");
        request.setReportTime(new Date());

        TaskSnapshotMessage message = new TaskSnapshotMessage();
        message.setJobId(id);
        message.setSnapshot("{\"offset\": 100}");
        request.setSnapshotList(Collections.singletonList(message));

        Boolean result = agentService.reportSnapshot(request);
        Assert.assertTrue(result);

        sourceService.delete(id, Constant.SOURCE_BINLOG, globalOperator);
    }

}
