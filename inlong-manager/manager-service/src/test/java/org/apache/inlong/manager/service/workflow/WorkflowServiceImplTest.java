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
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.enums.ProcessName;
import org.apache.inlong.manager.common.enums.ProcessStatus;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
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

import java.util.List;

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
    private static final String DATA_ENCODING = "UTF-8";

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

    protected String applicant;
    protected String subType = "default";
    private ProcessName processName;
    private GroupResourceProcessForm form;

    /**
     * Init inlong group form
     */
    public InlongGroupInfo createInlongGroup(String inlongGroupId, String mqType) {
        processName = ProcessName.CREATE_GROUP_RESOURCE;
        applicant = OPERATOR;
        form = new GroupResourceProcessForm();

        InlongGroupInfo groupInfo = super.createInlongGroup(inlongGroupId, mqType);
        form.setGroupInfo(groupInfo);
        form.setStreamInfos(Lists.newArrayList(super.createStreamInfo(groupInfo, STREAM_ID)));
        return groupInfo;
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
            public ListenerResult listen(WorkflowContext context) {
                return ListenerResult.success();
            }
        };
    }

    @Test
    public void testStartCreatePulsarWorkflow() {
        createInlongGroup("test14" + subType, MQType.MQ_PULSAR);
        mockTaskListenerFactory();

        WorkflowContext context = processService.start(processName.name(), applicant, form);
        WorkflowResult result = WorkflowBeanUtils.result(context);
        ProcessResponse processResponse = result.getProcessInfo();
        Assertions.assertSame(processResponse.getStatus(), ProcessStatus.COMPLETED);

        WorkflowProcess process = context.getProcess();
        WorkflowTask task = process.getTaskByName("InitMQ");
        Assertions.assertTrue(task instanceof ServiceTask);
        Assertions.assertEquals(2, task.getNameToListenerMap().size());

        List<TaskEventListener> listeners = Lists.newArrayList(task.getNameToListenerMap().values());
        Assertions.assertTrue(listeners.get(0) instanceof CreatePulsarGroupTaskListener);
        Assertions.assertTrue(listeners.get(1) instanceof CreatePulsarResourceTaskListener);

        // Integer processId = processResponse.getId();
        // context = processService.continueProcess(processId, applicant, "continue process");
        // result = WorkflowBeanUtils.result(context);
        // processResponse = result.getProcessInfo();
        // Assertions.assertSame(processResponse.getStatus(), ProcessStatus.COMPLETED);
    }

}
