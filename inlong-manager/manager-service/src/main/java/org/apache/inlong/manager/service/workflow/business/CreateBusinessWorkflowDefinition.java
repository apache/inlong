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

package org.apache.inlong.manager.service.workflow.business;

import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.dao.entity.DataStreamEntity;
import org.apache.inlong.manager.dao.mapper.DataStreamEntityMapper;
import org.apache.inlong.manager.service.core.StorageService;
import org.apache.inlong.manager.service.thirdpart.hive.CreateHiveTableListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreatePulsarGroupTaskListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreatePulsarResourceTaskListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreateTubeGroupTaskListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreateTubeTopicTaskListener;
import org.apache.inlong.manager.service.thirdpart.sort.PushHiveConfigTaskListener;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowDefinition;
import org.apache.inlong.manager.service.workflow.business.listener.BusinessCompleteProcessListener;
import org.apache.inlong.manager.service.workflow.business.listener.BusinessFailedProcessListener;
import org.apache.inlong.manager.service.workflow.business.listener.InitBusinessInfoListener;
import org.apache.inlong.manager.workflow.model.definition.EndEvent;
import org.apache.inlong.manager.workflow.model.definition.Process;
import org.apache.inlong.manager.workflow.model.definition.ServiceTask;
import org.apache.inlong.manager.workflow.model.definition.StartEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Create workflow definitions for business resources
 */
@Slf4j
@Component
public class CreateBusinessWorkflowDefinition implements WorkflowDefinition {

    @Autowired
    private InitBusinessInfoListener initBusinessInfoListener;
    @Autowired
    private BusinessCompleteProcessListener businessCompleteProcessListener;
    @Autowired
    private BusinessFailedProcessListener businessFailedProcessListener;
    @Autowired
    private CreateTubeTopicTaskListener createTubeTopicTaskListener;
    @Autowired
    private CreateTubeGroupTaskListener createTubeGroupTaskListener;
    @Autowired
    private CreatePulsarResourceTaskListener createPulsarResourceTaskListener;
    @Autowired
    private CreatePulsarGroupTaskListener createPulsarGroupTaskListener;

    @Autowired
    private CreateHiveTableListener createHiveTableListener;
    @Autowired
    private PushHiveConfigTaskListener pushHiveConfigTaskListener;
    @Autowired
    private StorageService storageService;
    @Autowired
    private DataStreamEntityMapper streamMapper;

    @Override
    public Process defineProcess() {

        // Configuration process
        Process process = new Process();
        process.addListener(initBusinessInfoListener);
        process.addListener(businessCompleteProcessListener);
        process.addListener(businessFailedProcessListener);

        process.setType("Business Resource Creation");
        process.setName(getProcessName().name());
        process.setDisplayName(getProcessName().getDisplayName());
        process.setFormClass(BusinessResourceWorkflowForm.class);
        process.setVersion(1);
        process.setHidden(true);

        // Start node
        StartEvent startEvent = new StartEvent();
        process.setStartEvent(startEvent);

        // End node
        EndEvent endEvent = new EndEvent();
        process.setEndEvent(endEvent);

        ServiceTask createTubeTopicTask = new ServiceTask();
        createTubeTopicTask.setSkipResolver(c -> {
            BusinessResourceWorkflowForm form = (BusinessResourceWorkflowForm) c.getProcessForm();
            BusinessInfo businessInfo = form.getBusinessInfo();
            if (BizConstant.MIDDLEWARE_TUBE.equalsIgnoreCase(businessInfo.getMiddlewareType())) {
                return false;
            }
            log.warn("not need to create tube resource for groupId={}, as the middleware type is {}",
                    businessInfo.getMiddlewareType(), form.getInlongGroupId());
            return true;
        });
        createTubeTopicTask.setName("createTubeTopic");
        createTubeTopicTask.setDisplayName("Business-CreateTubeTopic");
        createTubeTopicTask.addListener(createTubeTopicTaskListener);
        process.addTask(createTubeTopicTask);

        ServiceTask createTubeConsumerTask = new ServiceTask();
        createTubeConsumerTask.setSkipResolver(c -> {
            BusinessResourceWorkflowForm form = (BusinessResourceWorkflowForm) c.getProcessForm();
            String middlewareType = form.getBusinessInfo().getMiddlewareType();
            if (BizConstant.MIDDLEWARE_TUBE.equalsIgnoreCase(middlewareType)) {
                return false;
            }
            log.warn("no need to create tube resource for groupId={}, as the middleware type is {}",
                    form.getInlongGroupId(), middlewareType);
            return true;
        });
        createTubeConsumerTask.setName("createConsumerGroup");
        createTubeConsumerTask.setDisplayName("Business-CreateTubeConsumer");
        createTubeConsumerTask.addListener(createTubeGroupTaskListener);
        process.addTask(createTubeConsumerTask);

        ServiceTask createPulsarResourceTask = new ServiceTask();
        createPulsarResourceTask.setSkipResolver(c -> {
            BusinessResourceWorkflowForm form = (BusinessResourceWorkflowForm) c.getProcessForm();
            String middlewareType = form.getBusinessInfo().getMiddlewareType();
            if (BizConstant.MIDDLEWARE_PULSAR.equalsIgnoreCase(middlewareType)) {
                return false;
            }
            log.warn("no need to create pulsar resource for groupId={}, as the middlewareType={}",
                    form.getInlongGroupId(), middlewareType);
            return true;
        });
        createPulsarResourceTask.setName("createPulsarResource");
        createPulsarResourceTask.setDisplayName("Business-CreatePulsarResource");
        createPulsarResourceTask.addListener(createPulsarResourceTaskListener);
        process.addTask(createPulsarResourceTask);

        ServiceTask createPulsarSubscriptionTask = new ServiceTask();
        createPulsarSubscriptionTask.setSkipResolver(c -> {
            BusinessResourceWorkflowForm form = (BusinessResourceWorkflowForm) c.getProcessForm();
            String middlewareType = form.getBusinessInfo().getMiddlewareType();
            if (BizConstant.MIDDLEWARE_PULSAR.equalsIgnoreCase(middlewareType)) {
                return false;
            }
            log.warn("no need to create pulsar subscription group for groupId={}, as the middlewareType={}",
                    form.getInlongGroupId(), middlewareType);
            return true;
        });
        createPulsarSubscriptionTask.setName("createPulsarSubscriptionTask");
        createPulsarSubscriptionTask.setDisplayName("Business-CreatePulsarSubscription");
        createPulsarSubscriptionTask.addListener(createPulsarGroupTaskListener);
        process.addTask(createPulsarSubscriptionTask);

        ServiceTask createHiveTablesTask = new ServiceTask();
        createHiveTablesTask.setSkipResolver(c -> {
            BusinessResourceWorkflowForm form = (BusinessResourceWorkflowForm) c.getProcessForm();
            String groupId = form.getInlongGroupId();
            List<String> dsForHive = storageService.filterStreamIdByStorageType(groupId, BizConstant.STORAGE_HIVE,
                    streamMapper.selectByGroupId(groupId)
                            .stream()
                            .map(DataStreamEntity::getInlongStreamId)
                            .collect(Collectors.toList()));
            if (CollectionUtils.isEmpty(dsForHive)) {
                log.warn("groupId={} streamId={} does not have storage, skip to create hive table ",
                        groupId, form.getInlongStreamId());
                return true;
            }
            return false;
        });
        createHiveTablesTask.setName("createHiveTableTask");
        createHiveTablesTask.setDisplayName("Business-CreateHiveTable");
        createHiveTablesTask.addListener(createHiveTableListener);
        process.addTask(createHiveTablesTask);

        ServiceTask pushSortConfig = new ServiceTask();
        pushSortConfig.setName("pushSortConfig");
        pushSortConfig.setDisplayName("Business-PushSortConfig");
        pushSortConfig.addListener(pushHiveConfigTaskListener);
        process.addTask(pushSortConfig);

        startEvent.addNext(createTubeTopicTask);
        createTubeTopicTask.addNext(createTubeConsumerTask);
        createTubeConsumerTask.addNext(createPulsarResourceTask);
        createPulsarResourceTask.addNext(createPulsarSubscriptionTask);
        createPulsarSubscriptionTask.addNext(createHiveTablesTask);
        createHiveTablesTask.addNext(pushSortConfig);
        pushSortConfig.addNext(endEvent);

        return process;
    }

    @Override
    public ProcessName getProcessName() {
        return ProcessName.CREATE_BUSINESS_RESOURCE;
    }
}
