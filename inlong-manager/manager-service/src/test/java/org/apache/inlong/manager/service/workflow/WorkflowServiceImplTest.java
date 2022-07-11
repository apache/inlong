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

package org.apache.inlong.manager.service.workflow;

import com.google.common.collect.Lists;
import org.apache.inlong.manager.common.enums.FieldType;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.enums.ProcessStatus;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.none.InlongNoneMqInfo;
import org.apache.inlong.manager.common.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamRequest;
import org.apache.inlong.manager.common.pojo.stream.StreamField;
import org.apache.inlong.manager.common.pojo.workflow.ProcessResponse;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.common.pojo.workflow.form.process.GroupResourceProcessForm;
import org.apache.inlong.manager.dao.mapper.WorkflowProcessEntityMapper;
import org.apache.inlong.manager.dao.mapper.WorkflowTaskEntityMapper;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.mq.CreatePulsarGroupTaskListener;
import org.apache.inlong.manager.service.mq.CreatePulsarResourceTaskListener;
import org.apache.inlong.manager.service.mq.CreateTubeGroupTaskListener;
import org.apache.inlong.manager.service.mq.CreateTubeTopicTaskListener;
import org.apache.inlong.manager.service.resource.SinkResourceListener;
import org.apache.inlong.manager.service.workflow.listener.GroupTaskListenerFactory;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.core.ProcessService;
import org.apache.inlong.manager.workflow.definition.ServiceTask;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;
import org.apache.inlong.manager.workflow.definition.WorkflowTask;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.inlong.manager.workflow.event.task.TaskEventListener;
import org.apache.inlong.manager.workflow.util.WorkflowBeanUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class for workflow service.
 */
public class WorkflowServiceImplTest extends ServiceBaseTest {

    public static final String OPERATOR = "admin";

    public static final String GROUP_ID = "test_group";

    public static final String STREAM_ID = "test_stream";

    public static final String DATA_ENCODING = "UTF-8";

    protected String subType = "default";

    @Autowired
    protected WorkflowServiceImpl workflowService;
    @Autowired
    protected ProcessService processService;
    @Autowired
    protected InlongGroupService groupService;
    @Autowired
    protected GroupTaskListenerFactory taskListenerFactory;
    @Autowired
    protected WorkflowProcessEntityMapper processEntityMapper;
    @Autowired
    protected WorkflowTaskEntityMapper taskEntityMapper;
    @Autowired
    protected InlongStreamService streamService;

    protected ProcessName processName;

    protected String applicant;

    protected GroupResourceProcessForm form;

    protected final AtomicInteger tryTime = new AtomicInteger(0);

    /**
     * Init inlong group form
     */
    public InlongGroupInfo initGroupForm(String mqType, String groupId) {
        processName = ProcessName.CREATE_GROUP_RESOURCE;
        applicant = OPERATOR;

        try {
            streamService.logicDeleteAll(groupId, OPERATOR);
            groupService.delete(groupId, OPERATOR);
        } catch (Exception e) {
            // ignore
        }

        InlongGroupInfo groupInfo;
        if (MQType.forType(mqType) == MQType.PULSAR || MQType.forType(mqType) == MQType.TDMQ_PULSAR) {
            groupInfo = new InlongPulsarInfo();
        } else if (MQType.forType(mqType) == MQType.TUBE) {
            groupInfo = new InlongPulsarInfo();
        } else {
            groupInfo = new InlongNoneMqInfo();
        }

        groupInfo.setName(groupId);
        groupInfo.setInCharges(OPERATOR);
        groupInfo.setInlongGroupId(groupId);
        groupInfo.setMqType(mqType);
        groupInfo.setMqResource("test-queue");
        groupInfo.setEnableCreateResource(1);
        groupService.save(groupInfo.genRequest(), OPERATOR);

        groupService.updateStatus(groupId, GroupStatus.TO_BE_APPROVAL.getCode(), OPERATOR);
        groupService.updateStatus(groupId, GroupStatus.APPROVE_PASSED.getCode(), OPERATOR);
        groupService.update(groupInfo.genRequest(), OPERATOR);

        form = new GroupResourceProcessForm();
        form.setGroupInfo(groupInfo);
        form.setStreamInfos(Lists.newArrayList(createStreamInfo(groupInfo)));
        return groupInfo;
    }

    /**
     * Create inlong stream
     */
    public InlongStreamInfo createStreamInfo(InlongGroupInfo groupInfo) {
        // delete first
        try {
            streamService.delete(GROUP_ID, OPERATOR, OPERATOR);
        } catch (Exception e) {
            // ignore
        }

        InlongStreamRequest request = new InlongStreamRequest();
        request.setInlongGroupId(groupInfo.getInlongGroupId());
        request.setInlongStreamId(STREAM_ID);
        request.setMqResource(STREAM_ID);
        request.setDataSeparator("124");
        request.setDataEncoding(DATA_ENCODING);
        request.setFieldList(createStreamFields(groupInfo.getInlongGroupId(), STREAM_ID));
        streamService.save(request, OPERATOR);

        return streamService.get(request.getInlongGroupId(), request.getInlongStreamId());
    }

    public List<StreamField> createStreamFields(String groupId, String streamId) {
        final List<StreamField> streamFields = new ArrayList<>();
        StreamField fieldInfo = new StreamField();
        fieldInfo.setInlongGroupId(groupId);
        fieldInfo.setInlongStreamId(streamId);
        fieldInfo.setFieldName("id");
        fieldInfo.setFieldType(FieldType.INT.toString());
        fieldInfo.setFieldComment("idx");
        streamFields.add(fieldInfo);
        return streamFields;
    }

    /**
     * Mock the task listener factory
     */
    public void mockTaskListenerFactory() {
        CreateTubeGroupTaskListener createTubeGroupTaskListener = mock(CreateTubeGroupTaskListener.class);
        when(createTubeGroupTaskListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(createTubeGroupTaskListener.name()).thenReturn(SinkResourceListener.class.getSimpleName());
        when(createTubeGroupTaskListener.event()).thenReturn(TaskEvent.COMPLETE);
        taskListenerFactory.setCreateTubeGroupTaskListener(createTubeGroupTaskListener);

        CreateTubeTopicTaskListener createTubeTopicTaskListener = mock(CreateTubeTopicTaskListener.class);
        when(createTubeTopicTaskListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(createTubeTopicTaskListener.name()).thenReturn(CreateTubeTopicTaskListener.class.getSimpleName());
        when(createTubeTopicTaskListener.event()).thenReturn(TaskEvent.COMPLETE);
        taskListenerFactory.setCreateTubeTopicTaskListener(createTubeTopicTaskListener);

        CreatePulsarResourceTaskListener createPulsarResourceTaskListener = mock(
                CreatePulsarResourceTaskListener.class);
        when(createPulsarResourceTaskListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(createPulsarResourceTaskListener.name()).thenReturn(
                CreatePulsarResourceTaskListener.class.getSimpleName());
        when(createPulsarResourceTaskListener.event()).thenReturn(TaskEvent.COMPLETE);
        taskListenerFactory.setCreatePulsarResourceTaskListener(createPulsarResourceTaskListener);

        CreatePulsarGroupTaskListener createPulsarGroupTaskListener = mock(CreatePulsarGroupTaskListener.class);
        when(createPulsarGroupTaskListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(createPulsarGroupTaskListener.name()).thenReturn(CreatePulsarGroupTaskListener.class.getSimpleName());
        when(createPulsarGroupTaskListener.event()).thenReturn(TaskEvent.COMPLETE);
        taskListenerFactory.setCreatePulsarGroupTaskListener(createPulsarGroupTaskListener);

        SinkResourceListener sinkResourceListener = mock(SinkResourceListener.class);
        when(sinkResourceListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(sinkResourceListener.name()).thenReturn(SinkResourceListener.class.getSimpleName());
        when(sinkResourceListener.event()).thenReturn(TaskEvent.COMPLETE);
        taskListenerFactory.setSinkResourceListener(sinkResourceListener);

        taskListenerFactory.clearListeners();
        taskListenerFactory.init();
        SortOperateListener mockOperateListener = createMockSortListener();
        taskListenerFactory.getSortOperateListeners().put(mockOperateListener, context -> true);
    }

    public SortOperateListener createMockSortListener() {
        return new SortOperateListener() {

            @Override
            public TaskEvent event() {
                return TaskEvent.COMPLETE;
            }

            @Override
            public ListenerResult listen(WorkflowContext context) throws Exception {
                int tryTimes = tryTime.addAndGet(1);
                if (tryTimes % 2 == 1) {
                    throw new WorkflowListenerException();
                } else {
                    return ListenerResult.success();
                }
            }
        };
    }

    @Test
    public void testStartCreatePulsarWorkflow() throws Exception {
        initGroupForm(MQType.PULSAR.getType(), "test14" + subType);
        mockTaskListenerFactory();
        WorkflowContext context = processService.start(processName.name(), applicant, form);
        WorkflowResult result = WorkflowBeanUtils.result(context);
        ProcessResponse processResponse = result.getProcessInfo();
        // This method temporarily fails the test, so comment it out first
        Assertions.assertSame(processResponse.getStatus(), ProcessStatus.PROCESSING);
        WorkflowProcess process = context.getProcess();
        WorkflowTask task = process.getTaskByName("initMQ");
        Assertions.assertTrue(task instanceof ServiceTask);
        Assertions.assertEquals(2, task.getNameToListenerMap().size());

        List<TaskEventListener> listeners = Lists.newArrayList(task.getNameToListenerMap().values());
        Assertions.assertTrue(listeners.get(0) instanceof CreatePulsarGroupTaskListener);
        Assertions.assertTrue(listeners.get(1) instanceof CreatePulsarResourceTaskListener);

        Integer processId = processResponse.getId();
        context = processService.continueProcess(processId, applicant, "continue Process");
        result = WorkflowBeanUtils.result(context);
        processResponse = result.getProcessInfo();
        Assertions.assertSame(processResponse.getStatus(), ProcessStatus.COMPLETED);
    }

}
