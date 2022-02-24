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

package org.apache.inlong.manager.service.source.listener;

import com.google.common.collect.Lists;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.inlong.manager.common.enums.GroupState;
import org.apache.inlong.manager.common.enums.ProcessStatus;
import org.apache.inlong.manager.common.enums.SourceState;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.workflow.ProcessResponse;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm.OperateType;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowServiceImplTest;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.definition.ServiceTask;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;
import org.apache.inlong.manager.workflow.definition.WorkflowTask;
import org.apache.inlong.manager.workflow.event.task.TaskEventListener;
import org.apache.inlong.manager.workflow.util.WorkflowBeanUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class DataSourceListenerTest extends WorkflowServiceImplTest {

    public UpdateGroupProcessForm form;

    @Autowired
    private StreamSourceService streamSourceService;

    public BinlogSourceRequest createBinlogSourceRequest(InlongStreamInfo streamInfo) {
        BinlogSourceRequest binlogSourceRequest = new BinlogSourceRequest();
        binlogSourceRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        binlogSourceRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        binlogSourceRequest.setTableFields("id");
        binlogSourceRequest.setCharset(StandardCharsets.UTF_8.name());
        return binlogSourceRequest;
    }

    @Test
    public void testFrozenSource() {
        InlongGroupRequest groupRequest = initGroupForm("PULSAR");
        groupRequest.setStatus(GroupState.GROUP_CONFIG_SUCCESSFUL.getCode());
        groupService.update(groupRequest, OPERATOR);
        final InlongStreamInfo streamInfo = createStreamInfo(groupRequest);
        form = new UpdateGroupProcessForm();
        form.setGroupInfo(groupRequest);
        form.setOperateType(OperateType.SUSPEND);
        BinlogSourceRequest sourceRequest = createBinlogSourceRequest(streamInfo);
        int sourceId = streamSourceService.save(sourceRequest, OPERATOR);
        WorkflowContext context = workflowEngine.processService()
                .start(ProcessName.SUSPEND_GROUP_PROCESS.name(), applicant, form);
        WorkflowResult result = WorkflowBeanUtils.result(context);
        ProcessResponse response = result.getProcessInfo();
        Assert.assertSame(response.getStatus(), ProcessStatus.COMPLETED);
        WorkflowProcess process = context.getProcess();
        WorkflowTask task = process.getTaskByName("stopSource");
        Assert.assertTrue(task instanceof ServiceTask);
        Assert.assertEquals(1, task.getNameToListenerMap().size());
        List<TaskEventListener> listeners = Lists.newArrayList(task.getNameToListenerMap().values());
        Assert.assertTrue(listeners.get(0) instanceof SourceStopListener);
        SourceResponse sourceResponse = streamSourceService.get(sourceId, SourceType.DB_BINLOG.toString());
        Assert.assertTrue(SourceState.forCode(sourceResponse.getStatus()) == SourceState.SOURCE_FROZEN);
    }

    @Test
    public void testRestartSource() {
        InlongGroupRequest groupRequest = initGroupForm("PULSAR");
        groupRequest.setStatus(GroupState.GROUP_CONFIG_SUCCESSFUL.getCode());
        groupService.update(groupRequest, OPERATOR);
        groupRequest.setStatus(GroupState.GROUP_SUSPEND.getCode());
        groupService.update(groupRequest, OPERATOR);
        final InlongStreamInfo streamInfo = createStreamInfo(groupRequest);
        form = new UpdateGroupProcessForm();
        form.setGroupInfo(groupRequest);
        form.setOperateType(OperateType.RESTART);
        BinlogSourceRequest sourceRequest = createBinlogSourceRequest(streamInfo);
        int sourceId = streamSourceService.save(sourceRequest, OPERATOR);
        sourceRequest.setId(sourceId);
        sourceRequest.setStatus(SourceState.SOURCE_FROZEN.getCode());
        streamSourceService.update(sourceRequest, OPERATOR);
        WorkflowContext context = workflowEngine.processService()
                .start(ProcessName.SUSPEND_GROUP_PROCESS.name(), applicant, form);
        WorkflowResult result = WorkflowBeanUtils.result(context);
        ProcessResponse response = result.getProcessInfo();
        Assert.assertSame(response.getStatus(), ProcessStatus.COMPLETED);
        WorkflowProcess process = context.getProcess();
        WorkflowTask task = process.getTaskByName("stopSource");
        Assert.assertTrue(task instanceof ServiceTask);
        Assert.assertEquals(1, task.getNameToListenerMap().size());
        List<TaskEventListener> listeners = Lists.newArrayList(task.getNameToListenerMap().values());
        Assert.assertTrue(listeners.get(0) instanceof SourceRestartListener);
        SourceResponse sourceResponse = streamSourceService.get(sourceId, SourceType.DB_BINLOG.toString());
        Assert.assertTrue(SourceState.forCode(sourceResponse.getStatus()) == SourceState.SOURCE_ACTIVE);
    }
}
