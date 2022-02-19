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
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.enums.ProcessStatus;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessPulsarInfo;
import org.apache.inlong.manager.common.pojo.workflow.ProcessResponse;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.dao.entity.WorkflowProcessEntity;
import org.apache.inlong.manager.dao.entity.WorkflowTaskEntity;
import org.apache.inlong.manager.dao.mapper.WorkflowProcessEntityMapper;
import org.apache.inlong.manager.dao.mapper.WorkflowTaskEntityMapper;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.mocks.MockDeleteSortListener;
import org.apache.inlong.manager.service.mocks.MockDeleteSourceListener;
import org.apache.inlong.manager.service.mocks.MockPlugin;
import org.apache.inlong.manager.service.mocks.MockRestartSortListener;
import org.apache.inlong.manager.service.mocks.MockRestartSourceListener;
import org.apache.inlong.manager.service.mocks.MockStopSortListener;
import org.apache.inlong.manager.service.mocks.MockStopSourceListener;
import org.apache.inlong.manager.service.thirdpart.hive.CreateHiveTableListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreatePulsarGroupTaskListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreatePulsarResourceTaskListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreateTubeGroupTaskListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreateTubeTopicTaskListener;
import org.apache.inlong.manager.service.thirdpart.sort.PushHiveConfigTaskListener;
import org.apache.inlong.manager.service.workflow.business.BusinessResourceProcessForm;
import org.apache.inlong.manager.service.workflow.business.UpdateBusinessProcessForm;
import org.apache.inlong.manager.service.workflow.business.UpdateBusinessProcessForm.OperateType;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.base.WorkflowEngine;
import org.apache.inlong.manager.workflow.definition.ServiceTask;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;
import org.apache.inlong.manager.workflow.definition.WorkflowTask;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.inlong.manager.workflow.event.task.TaskEventListener;
import org.apache.inlong.manager.workflow.util.WorkflowBeanUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkflowServiceImplTest extends ServiceBaseTest {

    private static final String OPERATOR = "admin";

    @Autowired
    private WorkflowServiceImpl workflowService;
    @Autowired
    private WorkflowEngine workflowEngine;
    @Autowired
    private BusinessService businessService;
    @Autowired
    private ServiceTaskListenerFactory taskListenerFactory;
    @Autowired
    private WorkflowProcessEntityMapper processEntityMapper;
    @Autowired
    private WorkflowTaskEntityMapper taskEntityMapper;

    private ProcessName processName;

    private String applicant;

    private BusinessResourceProcessForm form;

    public BusinessInfo initBusinessForm(String middlewareType) {
        processName = ProcessName.CREATE_BUSINESS_RESOURCE;
        applicant = "test_create_new_business";
        form = new BusinessResourceProcessForm();
        BusinessInfo businessInfo = new BusinessInfo();
        String inlongStreamId = "test_stream";
        form.setInlongStreamId(inlongStreamId);
        form.setBusinessInfo(businessInfo);
        businessInfo.setName("test");
        businessInfo.setInlongGroupId("b_test");
        businessInfo.setMiddlewareType(middlewareType);
        businessInfo.setMqExtInfo(new BusinessPulsarInfo());
        businessInfo.setMqResourceObj("test-queue");
        businessService.save(businessInfo, OPERATOR);
        return businessInfo;
    }

    /**
     * Mock the task listener factory
     */
    public void mockTaskListenerFactory() {
        CreateTubeGroupTaskListener createTubeGroupTaskListener = mock(CreateTubeGroupTaskListener.class);
        when(createTubeGroupTaskListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(createTubeGroupTaskListener.name()).thenReturn(CreateHiveTableListener.class.getSimpleName());
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

        CreateHiveTableListener createHiveTableListener = mock(CreateHiveTableListener.class);
        when(createHiveTableListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(createHiveTableListener.name()).thenReturn(CreateHiveTableListener.class.getSimpleName());
        when(createHiveTableListener.event()).thenReturn(TaskEvent.COMPLETE);
        taskListenerFactory.setCreateHiveTableListener(createHiveTableListener);

        PushHiveConfigTaskListener pushHiveConfigTaskListener = mock(PushHiveConfigTaskListener.class);
        when(pushHiveConfigTaskListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(pushHiveConfigTaskListener.name()).thenReturn(PushHiveConfigTaskListener.class.getSimpleName());
        when(pushHiveConfigTaskListener.event()).thenReturn(TaskEvent.COMPLETE);
        taskListenerFactory.setPushHiveConfigTaskListener(pushHiveConfigTaskListener);
        taskListenerFactory.clearListeners();
        taskListenerFactory.init();
    }

    @Test
    public void testStartCreatePulsarWorkflow() {
        initBusinessForm(BizConstant.MIDDLEWARE_PULSAR);
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
        initBusinessForm(BizConstant.MIDDLEWARE_TUBE);
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
        BusinessInfo businessInfo = initBusinessForm(BizConstant.MIDDLEWARE_PULSAR);
        businessInfo.setStatus(EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode());
        businessService.update(businessInfo, OPERATOR);
        UpdateBusinessProcessForm form = new UpdateBusinessProcessForm();
        form.setBusinessInfo(businessInfo);
        form.setOperateType(OperateType.SUSPEND);
        taskListenerFactory.acceptPlugin(new MockPlugin());

        WorkflowContext context = workflowEngine.processService()
                .start(ProcessName.SUSPEND_BUSINESS_WORKFLOW.name(), applicant, form);
        WorkflowResult result = WorkflowBeanUtils.result(context);
        ProcessResponse response = result.getProcessInfo();
        Assert.assertSame(response.getStatus(), ProcessStatus.COMPLETED);

        WorkflowProcess process = context.getProcess();
        WorkflowTask stopSortTask = process.getTaskByName("stopSort");
        Assert.assertTrue(stopSortTask instanceof ServiceTask);
        List<TaskEventListener> listeners = Lists.newArrayList(stopSortTask.getNameToListenerMap().values());
        Assert.assertTrue(listeners.get(0) instanceof MockStopSortListener);

        WorkflowTask stopSourceTask = process.getTaskByName("stopDataSource");
        Assert.assertTrue(stopSourceTask instanceof ServiceTask);
        listeners = Lists.newArrayList(stopSourceTask.getNameToListenerMap().values());
        Assert.assertTrue(listeners.get(0) instanceof MockStopSourceListener);
    }

    @Test
    public void testRestartProcess() {
        BusinessInfo businessInfo = initBusinessForm(BizConstant.MIDDLEWARE_PULSAR);
        businessInfo.setStatus(EntityStatus.BIZ_SUSPEND.getCode());
        businessService.update(businessInfo, OPERATOR);
        UpdateBusinessProcessForm form = new UpdateBusinessProcessForm();
        form.setBusinessInfo(businessInfo);
        form.setOperateType(OperateType.RESTART);
        taskListenerFactory.acceptPlugin(new MockPlugin());

        WorkflowContext context = workflowEngine.processService()
                .start(ProcessName.RESTART_BUSINESS_WORKFLOW.name(), applicant, form);
        WorkflowResult result = WorkflowBeanUtils.result(context);
        ProcessResponse response = result.getProcessInfo();
        Assert.assertSame(response.getStatus(), ProcessStatus.COMPLETED);

        WorkflowProcess process = context.getProcess();
        WorkflowTask restartSort = process.getTaskByName("restartSort");
        Assert.assertTrue(restartSort instanceof ServiceTask);
        List<TaskEventListener> listeners = Lists.newArrayList(restartSort.getNameToListenerMap().values());
        Assert.assertEquals(1, listeners.size());
        Assert.assertTrue(listeners.get(0) instanceof MockRestartSortListener);

        WorkflowTask restartSourceTask = process.getTaskByName("restartDataSource");
        Assert.assertTrue(restartSourceTask instanceof ServiceTask);
        listeners = Lists.newArrayList(restartSourceTask.getNameToListenerMap().values());
        Assert.assertEquals(1, listeners.size());
        Assert.assertTrue(listeners.get(0) instanceof MockRestartSourceListener);
    }

    @Test
    public void testStopProcess() {
        BusinessInfo businessInfo = initBusinessForm(BizConstant.MIDDLEWARE_PULSAR);
        businessInfo.setStatus(EntityStatus.BIZ_RESTART.getCode());
        businessService.update(businessInfo, OPERATOR);
        UpdateBusinessProcessForm form = new UpdateBusinessProcessForm();
        form.setBusinessInfo(businessInfo);
        form.setOperateType(OperateType.DELETE);
        taskListenerFactory.acceptPlugin(new MockPlugin());

        WorkflowContext context = workflowEngine.processService()
                .start(ProcessName.DELETE_BUSINESS_WORKFLOW.name(), applicant, form);
        WorkflowResult result = WorkflowBeanUtils.result(context);
        ProcessResponse view = result.getProcessInfo();
        Assert.assertSame(view.getStatus(), ProcessStatus.COMPLETED);
        WorkflowProcess process = context.getProcess();
        WorkflowTask deleteSort = process.getTaskByName("deleteSort");
        Assert.assertTrue(deleteSort instanceof ServiceTask);
        List<TaskEventListener> listeners = Lists.newArrayList(deleteSort.getNameToListenerMap().values());
        Assert.assertEquals(1, listeners.size());
        Assert.assertTrue(listeners.get(0) instanceof MockDeleteSortListener);

        WorkflowTask deleteSourceTask = process.getTaskByName("deleteDataSource");
        Assert.assertTrue(deleteSourceTask instanceof ServiceTask);
        listeners = Lists.newArrayList(deleteSourceTask.getNameToListenerMap().values());
        Assert.assertEquals(1, listeners.size());
        Assert.assertTrue(listeners.get(0) instanceof MockDeleteSourceListener);
    }

    @Test
    public void testListTaskExecuteLogs() {
        // insert process instance
        String groupId = "test_business";
        WorkflowProcessEntity process = new WorkflowProcessEntity();
        process.setId(1);
        process.setInlongGroupId(groupId);
        process.setName("CREATE_BUSINESS_RESOURCE");
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
        query.setProcessNames(Collections.singletonList("CREATE_BUSINESS_RESOURCE"));
        PageInfo<WorkflowExecuteLog> logPageInfo = workflowService.listTaskExecuteLogs(query);

        Assert.assertEquals(1, logPageInfo.getTotal());
    }

}
