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

package org.apache.inlong.manager.service.thirdparty.sort;

import com.google.common.collect.Lists;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.inlong.manager.common.enums.GroupState;
import org.apache.inlong.manager.common.enums.ProcessStatus;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.sink.SinkFieldRequest;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkRequest;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.workflow.ProcessResponse;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.ProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm.OperateType;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.mocks.MockPlugin;
import org.apache.inlong.manager.service.sink.StreamSinkService;
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
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class DisableZkForSortTest extends WorkflowServiceImplTest {


    @Autowired
    protected InlongStreamService streamService;

    @Autowired
    protected StreamSinkService streamSinkService;

    @Autowired
    protected StreamSourceService streamSourceService;

    @Before
    public void init() {
        subType = "DisableZkFor";
    }

    public HiveSinkRequest createHiveSink(InlongStreamInfo streamInfo) {
        HiveSinkRequest hiveSinkRequest = new HiveSinkRequest();
        hiveSinkRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        hiveSinkRequest.setSinkType("HIVE");
        hiveSinkRequest.setSinkName("HIVE");
        hiveSinkRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        List<SinkFieldRequest> sinkFieldRequests = createStreamFields(streamInfo.getInlongGroupId(),
                streamInfo.getInlongStreamId())
                .stream()
                .map(streamFieldInfo -> {
                    SinkFieldRequest fieldInfo = new SinkFieldRequest();
                    fieldInfo.setFieldName(streamFieldInfo.getFieldName());
                    fieldInfo.setFieldType(streamFieldInfo.getFieldType());
                    fieldInfo.setFieldComment(streamFieldInfo.getFieldComment());
                    return fieldInfo;
                })
                .collect(Collectors.toList());
        hiveSinkRequest.setFieldList(sinkFieldRequests);
        hiveSinkRequest.setEnableCreateTable(0);
        hiveSinkRequest.setUsername(OPERATOR);
        hiveSinkRequest.setPassword("password");
        hiveSinkRequest.setDbName("default");
        hiveSinkRequest.setTableName("kip_test");
        hiveSinkRequest.setJdbcUrl("jdbc:hive2://172.17.12.135:7001");
        hiveSinkRequest.setFileFormat("TextFile");
        hiveSinkRequest.setHdfsDefaultFs("hdfs://172.17.12.235:4007");
        hiveSinkRequest.setWarehouseDir("/user/hive/warehouse");
        hiveSinkRequest.setFileFormat(StandardCharsets.UTF_8.name());
        hiveSinkRequest.setDataSeparator("124");
        streamSinkService.save(hiveSinkRequest, OPERATOR);
        return hiveSinkRequest;
    }

    public KafkaSourceRequest createKafkaSource(InlongStreamInfo streamInfo) {
        KafkaSourceRequest kafkaSourceRequest = new KafkaSourceRequest();
        kafkaSourceRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        kafkaSourceRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        kafkaSourceRequest.setGroupId("default");
        kafkaSourceRequest.setSerializationType("csv");
        kafkaSourceRequest.setSourceName("KAFKA");
        streamSourceService.save(kafkaSourceRequest, OPERATOR);
        return kafkaSourceRequest;
    }

    @Test
    public void testCreateSortConfigInCreateWorkflow() {
        InlongGroupInfo groupInfo = initGroupForm("PULSAR", "test21");
        groupInfo.setStatus(GroupState.GROUP_CONFIG_SUCCESSFUL.getCode());
        groupInfo.setZookeeperEnabled(0);
        groupService.update(groupInfo.genRequest(), OPERATOR);
        InlongStreamInfo streamInfo = createStreamInfo(groupInfo);
        createHiveSink(streamInfo);
        createKafkaSource(streamInfo);
        mockTaskListenerFactory();
        WorkflowContext context = workflowEngine.processService().start(processName.name(), applicant, form);
        WorkflowResult result = WorkflowBeanUtils.result(context);
        ProcessResponse response = result.getProcessInfo();
        Assert.assertSame(response.getStatus(), ProcessStatus.COMPLETED);
        WorkflowProcess process = context.getProcess();
        WorkflowTask task = process.getTaskByName("initSort");
        Assert.assertTrue(task instanceof ServiceTask);
        Assert.assertEquals(1, task.getNameToListenerMap().size());

        List<TaskEventListener> listeners = Lists.newArrayList(task.getNameToListenerMap().values());
        Assert.assertTrue(listeners.get(0) instanceof CreateSortConfigListener);
        ProcessForm form = context.getProcessForm();
        InlongGroupInfo curGroupRequest = ((GroupResourceProcessForm) form).getGroupInfo();
        Assert.assertTrue(curGroupRequest.getExtList().size() == 1);

    }

    //    @Test
    public void testCreateSortConfigInUpdateWorkflow() {
        InlongGroupInfo groupInfo = initGroupForm("PULSAR", "test20");
        groupInfo.setZookeeperEnabled(0);
        groupInfo.setStatus(GroupState.GROUP_CONFIG_SUCCESSFUL.getCode());
        groupService.update(groupInfo.genRequest(), OPERATOR);
        InlongStreamInfo streamInfo = createStreamInfo(groupInfo);
        createHiveSink(streamInfo);
        createKafkaSource(streamInfo);
        UpdateGroupProcessForm form = new UpdateGroupProcessForm();
        form.setGroupInfo(groupInfo);
        form.setOperateType(OperateType.SUSPEND);
        taskListenerFactory.acceptPlugin(new MockPlugin());

        WorkflowContext context = workflowEngine.processService()
                .start(ProcessName.SUSPEND_GROUP_PROCESS.name(), applicant, form);
        WorkflowResult result = WorkflowBeanUtils.result(context);
        ProcessResponse response = result.getProcessInfo();
        Assert.assertSame(response.getStatus(), ProcessStatus.COMPLETED);
        WorkflowProcess process = context.getProcess();
        WorkflowTask task = process.getTaskByName("stopSort");
        Assert.assertTrue(task instanceof ServiceTask);
        Assert.assertEquals(2, task.getNameToListenerMap().size());
        List<TaskEventListener> listeners = Lists.newArrayList(task.getNameToListenerMap().values());
        Assert.assertTrue(listeners.get(1) instanceof CreateSortConfigListener);
        ProcessForm currentProcessForm = context.getProcessForm();
        InlongGroupInfo curGroupRequest = ((UpdateGroupProcessForm) currentProcessForm).getGroupInfo();
        Assert.assertTrue(curGroupRequest.getExtList().size() == 1);
    }

}
