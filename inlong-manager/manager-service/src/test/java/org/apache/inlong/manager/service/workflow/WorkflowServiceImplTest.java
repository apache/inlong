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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.event.ListenerResult;
import org.apache.inlong.manager.common.event.task.TaskEvent;
import org.apache.inlong.manager.common.event.task.TaskEventListener;
import org.apache.inlong.manager.common.model.ProcessState;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.definition.Process;
import org.apache.inlong.manager.common.model.definition.ServiceTask;
import org.apache.inlong.manager.common.model.definition.Task;
import org.apache.inlong.manager.common.model.view.ProcessView;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessPulsarInfo;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.service.BaseTest;
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
import org.apache.inlong.manager.service.workflow.business.BusinessResourceWorkflowForm;
import org.apache.inlong.manager.service.workflow.business.UpdateBusinessWorkflowForm;
import org.apache.inlong.manager.service.workflow.business.UpdateBusinessWorkflowForm.OperateType;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class WorkflowServiceImplTest extends BaseTest {

    private static final String OPERATOR = "admin";

    @Autowired
    WorkflowServiceImpl workflowService;

    @Autowired
    BusinessService businessService;

    @Autowired
    ServiceTaskListenerFactory serviceTaskListenerFactory;

    private ProcessName processName;

    private String applicant;

    private BusinessResourceWorkflowForm form;

    public BusinessInfo initBusinessForm(String middlewareType) {
        processName = ProcessName.CREATE_BUSINESS_RESOURCE;
        applicant = "test_create_new_business";
        form = new BusinessResourceWorkflowForm();
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

    public void mockTaskListenerFactory() {
        CreateTubeGroupTaskListener createTubeGroupTaskListener = mock(CreateTubeGroupTaskListener.class);
        when(createTubeGroupTaskListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(createTubeGroupTaskListener.name()).thenReturn(CreateHiveTableListener.class.getSimpleName());
        when(createTubeGroupTaskListener.event()).thenReturn(TaskEvent.COMPLETE);
        serviceTaskListenerFactory.setCreateTubeGroupTaskListener(createTubeGroupTaskListener);

        CreateTubeTopicTaskListener createTubeTopicTaskListener = mock(CreateTubeTopicTaskListener.class);
        when(createTubeTopicTaskListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(createTubeTopicTaskListener.name()).thenReturn(CreateTubeTopicTaskListener.class.getSimpleName());
        when(createTubeTopicTaskListener.event()).thenReturn(TaskEvent.COMPLETE);
        serviceTaskListenerFactory.setCreateTubeTopicTaskListener(createTubeTopicTaskListener);

        CreatePulsarResourceTaskListener createPulsarResourceTaskListener = mock(
                CreatePulsarResourceTaskListener.class);
        when(createPulsarResourceTaskListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(createPulsarResourceTaskListener.name()).thenReturn(
                CreatePulsarResourceTaskListener.class.getSimpleName());
        when(createPulsarResourceTaskListener.event()).thenReturn(TaskEvent.COMPLETE);
        serviceTaskListenerFactory.setCreatePulsarResourceTaskListener(createPulsarResourceTaskListener);

        CreatePulsarGroupTaskListener createPulsarGroupTaskListener = mock(CreatePulsarGroupTaskListener.class);
        when(createPulsarGroupTaskListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(createPulsarGroupTaskListener.name()).thenReturn(CreatePulsarGroupTaskListener.class.getSimpleName());
        when(createPulsarGroupTaskListener.event()).thenReturn(TaskEvent.COMPLETE);
        serviceTaskListenerFactory.setCreatePulsarGroupTaskListener(createPulsarGroupTaskListener);

        CreateHiveTableListener createHiveTableListener = mock(CreateHiveTableListener.class);
        when(createHiveTableListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(createHiveTableListener.name()).thenReturn(CreateHiveTableListener.class.getSimpleName());
        when(createHiveTableListener.event()).thenReturn(TaskEvent.COMPLETE);
        serviceTaskListenerFactory.setCreateHiveTableListener(createHiveTableListener);

        PushHiveConfigTaskListener pushHiveConfigTaskListener = mock(PushHiveConfigTaskListener.class);
        when(pushHiveConfigTaskListener.listen(any(WorkflowContext.class))).thenReturn(ListenerResult.success());
        when(pushHiveConfigTaskListener.name()).thenReturn(PushHiveConfigTaskListener.class.getSimpleName());
        when(pushHiveConfigTaskListener.event()).thenReturn(TaskEvent.COMPLETE);
        serviceTaskListenerFactory.setPushHiveConfigTaskListener(pushHiveConfigTaskListener);
        serviceTaskListenerFactory.clearListeners();
        serviceTaskListenerFactory.init();
    }

    @Test
    public void testStartCreatePulsarWorkflow() {
        initBusinessForm(BizConstant.MIDDLEWARE_PULSAR);
        mockTaskListenerFactory();
        WorkflowContext context = workflowService.getWorkflowEngine().processService()
                .start(processName.name(), applicant, form);
        WorkflowResult result = WorkflowResult.of(context);
        ProcessView view = result.getProcessInfo();
        Assert.assertTrue(view.getState() == ProcessState.COMPLETED);
        Process process = context.getProcess();
        Task task = process.getTaskByName("initMQ");
        Assert.assertTrue(task instanceof ServiceTask);
        Assert.assertTrue(task.getName2EventListenerMap().size() == 2);
        List<TaskEventListener> listeners = Lists.newArrayList(task.getName2EventListenerMap().values());
        Assert.assertTrue(listeners.get(0) instanceof CreatePulsarGroupTaskListener);
        Assert.assertTrue(listeners.get(1) instanceof CreatePulsarResourceTaskListener);
    }

    @Test
    public void testStartCreateTubeWorkflow() {
        initBusinessForm(BizConstant.MIDDLEWARE_TUBE);
        mockTaskListenerFactory();
        WorkflowContext context = workflowService.getWorkflowEngine().processService()
                .start(processName.name(), applicant, form);
        WorkflowResult result = WorkflowResult.of(context);
        ProcessView view = result.getProcessInfo();
        Assert.assertTrue(view.getState() == ProcessState.COMPLETED);
        Process process = context.getProcess();
        Task task = process.getTaskByName("initMQ");
        Assert.assertTrue(task instanceof ServiceTask);
        Assert.assertTrue(task.getName2EventListenerMap().size() == 2);
        List<TaskEventListener> listeners = Lists.newArrayList(task.getName2EventListenerMap().values());
        Assert.assertTrue(listeners.get(0) instanceof CreateTubeTopicTaskListener);
        Assert.assertTrue(listeners.get(1) instanceof CreateTubeGroupTaskListener);
    }

    @Test
    public void testSuspendProcess() {
        BusinessInfo businessInfo = initBusinessForm(BizConstant.MIDDLEWARE_PULSAR);
        businessInfo.setStatus(EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode());
        businessService.update(businessInfo, OPERATOR);
        UpdateBusinessWorkflowForm form = new UpdateBusinessWorkflowForm();
        form.setBusinessInfo(businessInfo);
        form.setOperateType(OperateType.SUSPEND);
        serviceTaskListenerFactory.acceptPlugin(new MockPlugin());
        WorkflowContext context = workflowService.getWorkflowEngine().processService()
                .start(ProcessName.SUSPEND_BUSINESS_WORKFLOW.name(), applicant, form);
        WorkflowResult result = WorkflowResult.of(context);
        ProcessView view = result.getProcessInfo();
        Assert.assertTrue(view.getState() == ProcessState.COMPLETED);
        Process process = context.getProcess();
        Task stopSortTask = process.getTaskByName("stopSort");
        Assert.assertTrue(stopSortTask instanceof ServiceTask);
        List<TaskEventListener> listeners = Lists.newArrayList(stopSortTask.getName2EventListenerMap().values());
        Assert.assertTrue(listeners.get(0) instanceof MockStopSortListener);
        Task stopSourceTask = process.getTaskByName("stopDataSource");
        Assert.assertTrue(stopSourceTask instanceof ServiceTask);
        listeners = Lists.newArrayList(stopSourceTask.getName2EventListenerMap().values());
        Assert.assertTrue(listeners.get(0) instanceof MockStopSourceListener);
    }

    @Test
    public void testRestartProcess() {
        BusinessInfo businessInfo = initBusinessForm(BizConstant.MIDDLEWARE_PULSAR);
        businessInfo.setStatus(EntityStatus.BIZ_SUSPEND.getCode());
        businessService.update(businessInfo, OPERATOR);
        UpdateBusinessWorkflowForm form = new UpdateBusinessWorkflowForm();
        form.setBusinessInfo(businessInfo);
        form.setOperateType(OperateType.RESTART);
        serviceTaskListenerFactory.acceptPlugin(new MockPlugin());
        WorkflowContext context = workflowService.getWorkflowEngine().processService()
                .start(ProcessName.RESTART_BUSINESS_WORKFLOW.name(), applicant, form);
        WorkflowResult result = WorkflowResult.of(context);
        ProcessView view = result.getProcessInfo();
        Assert.assertTrue(view.getState() == ProcessState.COMPLETED);
        Process process = context.getProcess();
        Task restartSort = process.getTaskByName("restartSort");
        Assert.assertTrue(restartSort instanceof ServiceTask);
        List<TaskEventListener> listeners = Lists.newArrayList(restartSort.getName2EventListenerMap().values());
        Assert.assertTrue(listeners.size() == 1);
        Assert.assertTrue(listeners.get(0) instanceof MockRestartSortListener);
        Task restartSourceTask = process.getTaskByName("restartDataSource");
        Assert.assertTrue(restartSourceTask instanceof ServiceTask);
        listeners = Lists.newArrayList(restartSourceTask.getName2EventListenerMap().values());
        Assert.assertTrue(listeners.size() == 1);
        Assert.assertTrue(listeners.get(0) instanceof MockRestartSourceListener);
    }

    @Test
    public void testStopProcess() {
        BusinessInfo businessInfo = initBusinessForm(BizConstant.MIDDLEWARE_PULSAR);
        businessInfo.setStatus(EntityStatus.BIZ_RESTART.getCode());
        businessService.update(businessInfo, OPERATOR);
        UpdateBusinessWorkflowForm form = new UpdateBusinessWorkflowForm();
        form.setBusinessInfo(businessInfo);
        form.setOperateType(OperateType.DELETE);
        serviceTaskListenerFactory.acceptPlugin(new MockPlugin());
        WorkflowContext context = workflowService.getWorkflowEngine().processService()
                .start(ProcessName.DELETE_BUSINESS_WORKFLOW.name(), applicant, form);
        WorkflowResult result = WorkflowResult.of(context);
        ProcessView view = result.getProcessInfo();
        Assert.assertTrue(view.getState() == ProcessState.COMPLETED);
        Process process = context.getProcess();
        Task deleteSort = process.getTaskByName("deleteSort");
        Assert.assertTrue(deleteSort instanceof ServiceTask);
        List<TaskEventListener> listeners = Lists.newArrayList(deleteSort.getName2EventListenerMap().values());
        Assert.assertTrue(listeners.size() == 1);
        Assert.assertTrue(listeners.get(0) instanceof MockDeleteSortListener);
        Task deleteSourceTask = process.getTaskByName("deleteDataSource");
        Assert.assertTrue(deleteSourceTask instanceof ServiceTask);
        listeners = Lists.newArrayList(deleteSourceTask.getName2EventListenerMap().values());
        Assert.assertTrue(listeners.size() == 1);
        Assert.assertTrue(listeners.get(0) instanceof MockDeleteSourceListener);
    }

}
