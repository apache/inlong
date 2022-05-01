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

import com.github.pagehelper.PageInfo;
import com.google.common.collect.Lists;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.ProcessStatus;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPulsarInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamRequest;
import org.apache.inlong.manager.common.pojo.workflow.ProcessResponse;
import org.apache.inlong.manager.common.pojo.workflow.TaskExecuteLogQuery;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm;
import org.apache.inlong.manager.dao.entity.WorkflowProcessEntity;
import org.apache.inlong.manager.dao.entity.WorkflowTaskEntity;
import org.apache.inlong.manager.dao.mapper.WorkflowProcessEntityMapper;
import org.apache.inlong.manager.dao.mapper.WorkflowTaskEntityMapper;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.InlongGroupService;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.mocks.MockDeleteSortListener;
import org.apache.inlong.manager.service.mocks.MockPlugin;
import org.apache.inlong.manager.service.mocks.MockRestartSortListener;
import org.apache.inlong.manager.service.mocks.MockStopSortListener;
import org.apache.inlong.manager.service.mq.CreatePulsarGroupTaskListener;
import org.apache.inlong.manager.service.mq.CreatePulsarResourceTaskListener;
import org.apache.inlong.manager.service.mq.CreateTubeGroupTaskListener;
import org.apache.inlong.manager.service.mq.CreateTubeTopicTaskListener;
import org.apache.inlong.manager.service.resource.CreateSinkResourceListener;
import org.apache.inlong.manager.service.sort.PushSortConfigListener;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.core.WorkflowEngine;
import org.apache.inlong.manager.workflow.definition.ServiceTask;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;
import org.apache.inlong.manager.workflow.definition.WorkflowTask;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.inlong.manager.workflow.event.task.TaskEventListener;
import org.apache.inlong.manager.workflow.util.WorkflowBeanUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import springfox.boot.starter.autoconfigure.OpenApiAutoConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
@EnableAutoConfiguration(exclude = OpenApiAutoConfiguration.class)
public class WorkflowServiceImplTest extends ServiceBaseTest {

    public static final String OPERATOR = "admin";

    public static final String GROUP_ID = "b_test";

    public static final String STREAM_ID = "test_stream";

    public static final String DATA_ENCODING = "UTF-8";

    @Autowired
    protected WorkflowServiceImpl workflowService;
    @Autowired
    protected WorkflowEngine workflowEngine;
    @Autowired
    protected InlongGroupService groupService;
    @Autowired
    protected ServiceTaskListenerFactory taskListenerFactory;
    @Autowired
    protected WorkflowProcessEntityMapper processEntityMapper;
    @Autowired
    protected WorkflowTaskEntityMapper taskEntityMapper;
    @Autowired
    protected InlongStreamService streamService;

    protected ProcessName processName;

    protected String applicant;

    protected GroupResourceProcessForm form;

    /**
     * Init inlong group form
     */
    public InlongGroupInfo initGroupForm(String middlewareType) {
        processName = ProcessName.CREATE_GROUP_RESOURCE;
        applicant = OPERATOR;

        try {
            streamService.logicDeleteAll(GROUP_ID, OPERATOR);
            groupService.delete(GROUP_ID, OPERATOR);
        } catch (Exception e) {
            // ignore
        }

        InlongGroupInfo groupInfo = new InlongGroupInfo();
        groupInfo.setName("test");
        groupInfo.setInCharges(OPERATOR);
        groupInfo.setInlongGroupId(GROUP_ID);
        groupInfo.setMiddlewareType(middlewareType);
        groupInfo.setMqExtInfo(new InlongGroupPulsarInfo());
        groupInfo.setMqResourceObj("test-queue");
        groupService.save(groupInfo.genRequest(), OPERATOR);

        groupService.updateStatus(GROUP_ID, GroupStatus.TO_BE_APPROVAL.getCode(), OPERATOR);
        groupService.update(groupInfo.genRequest(), OPERATOR);
        groupService.updateStatus(GROUP_ID, GroupStatus.APPROVE_PASSED.getCode(), OPERATOR);
        groupService.update(groupInfo.genRequest(), OPERATOR);
        groupService.updateStatus(GROUP_ID, GroupStatus.CONFIG_ING.getCode(), OPERATOR);
        groupService.update(groupInfo.genRequest(), OPERATOR);

        form = new GroupResourceProcessForm();
        form.setGroupInfo(groupInfo);
        return groupInfo;
    }

    /**
     * Delete inlong group.
     */
    public void deleteGroupInfo() {
        groupService.delete(GROUP_ID, OPERATOR);
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
        request.setMqResourceObj(STREAM_ID);
        request.setDataSeparator("124");
        request.setDataEncoding(DATA_ENCODING);
        request.setInCharges(OPERATOR);
        request.setFieldList(createStreamFields(groupInfo.getInlongGroupId(), STREAM_ID));
        streamService.save(request, OPERATOR);

        return streamService.get(request.getInlongGroupId(), request.getInlongStreamId());
    }

    public List<InlongStreamFieldInfo> createStreamFields(String groupId, String streamId) {
        final List<InlongStreamFieldInfo> streamFieldInfos = new ArrayList<>();
        InlongStreamFieldInfo fieldInfo = new InlongStreamFieldInfo();
        fieldInfo.setInlongGroupId(groupId);
        fieldInfo.setInlongStreamId(streamId);
        fieldInfo.setFieldName("id");
        fieldInfo.setFieldType("int");
        fieldInfo.setFieldComment("idx");
        streamFieldInfos.add(fieldInfo);
        return streamFieldInfos;
    }

    /**
     * Mock the task listener factory
     */
    public void mockTaskListenerFactory() {
        CreateTubeGroupTaskListener createTubeGroupTaskListener = mock(CreateTubeGroupTaskListener.class);
        when(createTubeGroupTaskListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(createTubeGroupTaskListener.name()).thenReturn(CreateSinkResourceListener.class.getSimpleName());
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

        CreateSinkResourceListener createSinkResourceListener = mock(CreateSinkResourceListener.class);
        when(createSinkResourceListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(createSinkResourceListener.name()).thenReturn(CreateSinkResourceListener.class.getSimpleName());
        when(createSinkResourceListener.event()).thenReturn(TaskEvent.COMPLETE);
        taskListenerFactory.setCreateSinkResourceListener(createSinkResourceListener);

        PushSortConfigListener pushSortConfigListener = mock(PushSortConfigListener.class);
        when(pushSortConfigListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(pushSortConfigListener.name()).thenReturn(PushSortConfigListener.class.getSimpleName());
        when(pushSortConfigListener.event()).thenReturn(TaskEvent.COMPLETE);
        taskListenerFactory.setPushSortConfigListener(pushSortConfigListener);
        taskListenerFactory.clearListeners();
        taskListenerFactory.init();
    }

    @Test
    public void testStartCreatePulsarWorkflow() {
        initGroupForm(MQType.PULSAR.getType());
        mockTaskListenerFactory();
        WorkflowContext context = workflowEngine.processService().start(processName.name(), applicant, form);
        WorkflowResult result = WorkflowBeanUtils.result(context);
        ProcessResponse view = result.getProcessInfo();
        Assert.assertSame(view.getStatus(), ProcessStatus.COMPLETED);
        WorkflowProcess process = context.getProcess();
        WorkflowTask task = process.getTaskByName("initMQ");
        Assert.assertTrue(task instanceof ServiceTask);
        Assert.assertEquals(2, task.getNameToListenerMap().size());

        List<TaskEventListener> listeners = Lists.newArrayList(task.getNameToListenerMap().values());
        Assert.assertTrue(listeners.get(0) instanceof CreatePulsarGroupTaskListener);
        Assert.assertTrue(listeners.get(1) instanceof CreatePulsarResourceTaskListener);
    }

    @Test
    public void testStartCreateTubeWorkflow() {
        initGroupForm(MQType.TUBE.getType());
        mockTaskListenerFactory();
        WorkflowContext context = workflowEngine.processService().start(processName.name(), applicant, form);
        WorkflowResult result = WorkflowBeanUtils.result(context);
        ProcessResponse response = result.getProcessInfo();
        Assert.assertSame(response.getStatus(), ProcessStatus.COMPLETED);

        WorkflowProcess process = context.getProcess();
        WorkflowTask task = process.getTaskByName("initMQ");
        Assert.assertTrue(task instanceof ServiceTask);
        Assert.assertEquals(2, task.getNameToListenerMap().size());

        List<TaskEventListener> listeners = Lists.newArrayList(task.getNameToListenerMap().values());
        Assert.assertTrue(listeners.get(0) instanceof CreateTubeTopicTaskListener);
        Assert.assertTrue(listeners.get(1) instanceof CreateTubeGroupTaskListener);
    }

    @Test
    public void testSuspendProcess() {
        InlongGroupInfo groupInfo = initGroupForm(MQType.PULSAR.getType());
        groupService.updateStatus(GROUP_ID, GroupStatus.CONFIG_SUCCESSFUL.getCode(), OPERATOR);
        groupService.update(groupInfo.genRequest(), OPERATOR);

        UpdateGroupProcessForm form = new UpdateGroupProcessForm();
        form.setGroupInfo(groupInfo);
        form.setGroupOperateType(GroupOperateType.SUSPEND);
        taskListenerFactory.acceptPlugin(new MockPlugin());

        WorkflowContext context = workflowEngine.processService()
                .start(ProcessName.SUSPEND_GROUP_PROCESS.name(), applicant, form);
        WorkflowResult result = WorkflowBeanUtils.result(context);
        ProcessResponse response = result.getProcessInfo();
        Assert.assertSame(response.getStatus(), ProcessStatus.COMPLETED);

        WorkflowProcess process = context.getProcess();
        WorkflowTask stopSortTask = process.getTaskByName("stopSort");
        Assert.assertTrue(stopSortTask instanceof ServiceTask);
        List<TaskEventListener> listeners = Lists.newArrayList(stopSortTask.getNameToListenerMap().values());
        Assert.assertTrue(listeners.get(0) instanceof MockStopSortListener);

        WorkflowTask stopSourceTask = process.getTaskByName("stopSource");
        Assert.assertTrue(stopSourceTask instanceof ServiceTask);
        listeners = Lists.newArrayList(stopSourceTask.getNameToListenerMap().values());
    }

    @Test
    public void testRestartProcess() {
        InlongGroupInfo groupInfo = initGroupForm(MQType.PULSAR.getType());
        groupService.updateStatus(GROUP_ID, GroupStatus.CONFIG_SUCCESSFUL.getCode(), OPERATOR);
        groupService.update(groupInfo.genRequest(), OPERATOR);
        groupService.updateStatus(GROUP_ID, GroupStatus.SUSPENDED.getCode(), OPERATOR);
        groupService.update(groupInfo.genRequest(), OPERATOR);

        UpdateGroupProcessForm form = new UpdateGroupProcessForm();
        form.setGroupInfo(groupInfo);
        form.setGroupOperateType(GroupOperateType.RESTART);
        taskListenerFactory.acceptPlugin(new MockPlugin());

        WorkflowContext context = workflowEngine.processService()
                .start(ProcessName.RESTART_GROUP_PROCESS.name(), applicant, form);
        WorkflowResult result = WorkflowBeanUtils.result(context);
        ProcessResponse response = result.getProcessInfo();
        Assert.assertSame(response.getStatus(), ProcessStatus.COMPLETED);

        WorkflowProcess process = context.getProcess();
        WorkflowTask restartSort = process.getTaskByName("restartSort");
        Assert.assertTrue(restartSort instanceof ServiceTask);
        List<TaskEventListener> listeners = Lists.newArrayList(restartSort.getNameToListenerMap().values());
        Assert.assertEquals(1, listeners.size());
        Assert.assertTrue(listeners.get(0) instanceof MockRestartSortListener);

        WorkflowTask restartSourceTask = process.getTaskByName("restartSource");
        Assert.assertTrue(restartSourceTask instanceof ServiceTask);
        listeners = Lists.newArrayList(restartSourceTask.getNameToListenerMap().values());
        Assert.assertEquals(2, listeners.size());
    }

    @Test
    public void testStopProcess() {
        InlongGroupInfo groupInfo = initGroupForm(MQType.PULSAR.getType());

        groupService.updateStatus(GROUP_ID, GroupStatus.CONFIG_SUCCESSFUL.getCode(), OPERATOR);
        groupService.update(groupInfo.genRequest(), OPERATOR);
        groupService.updateStatus(GROUP_ID, GroupStatus.SUSPENDED.getCode(), OPERATOR);
        groupService.update(groupInfo.genRequest(), OPERATOR);

        UpdateGroupProcessForm form = new UpdateGroupProcessForm();
        form.setGroupInfo(groupInfo);
        form.setGroupOperateType(GroupOperateType.DELETE);
        taskListenerFactory.acceptPlugin(new MockPlugin());

        WorkflowContext context = workflowEngine.processService()
                .start(ProcessName.DELETE_GROUP_PROCESS.name(), applicant, form);
        WorkflowResult result = WorkflowBeanUtils.result(context);
        ProcessResponse view = result.getProcessInfo();
        Assert.assertSame(view.getStatus(), ProcessStatus.COMPLETED);
        WorkflowProcess process = context.getProcess();
        WorkflowTask deleteSort = process.getTaskByName("deleteSort");
        Assert.assertTrue(deleteSort instanceof ServiceTask);
        List<TaskEventListener> listeners = Lists.newArrayList(deleteSort.getNameToListenerMap().values());
        Assert.assertEquals(1, listeners.size());
        Assert.assertTrue(listeners.get(0) instanceof MockDeleteSortListener);

        WorkflowTask deleteSourceTask = process.getTaskByName("deleteSource");
        Assert.assertTrue(deleteSourceTask instanceof ServiceTask);
        listeners = Lists.newArrayList(deleteSourceTask.getNameToListenerMap().values());
        Assert.assertEquals(2, listeners.size());
    }

    @Test
    public void testListTaskExecuteLogs() {
        // insert process instance
        String groupId = "test_group";
        WorkflowProcessEntity process = new WorkflowProcessEntity();
        process.setId(1);
        process.setInlongGroupId(groupId);
        process.setName("CREATE_GROUP_RESOURCE");
        process.setDisplayName("Group-Resource");
        process.setHidden(1);
        process.setStatus(ProcessStatus.COMPLETED.name());
        processEntityMapper.insert(process);

        // insert task instance
        WorkflowTaskEntity task = new WorkflowTaskEntity();
        task.setId(1);
        task.setType("ServiceTask");
        task.setProcessId(1);
        taskEntityMapper.insert(task);

        // query execute logs
        TaskExecuteLogQuery query = new TaskExecuteLogQuery();
        query.setInlongGroupId(groupId);
        query.setProcessNames(Collections.singletonList("CREATE_GROUP_RESOURCE"));
        PageInfo<WorkflowExecuteLog> logPageInfo = workflowService.listTaskExecuteLogs(query);

        Assert.assertEquals(1, logPageInfo.getTotal());
    }

}
