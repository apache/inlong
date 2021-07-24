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

package org.apache.inlong.manager.service.workflow.newbusiness;

import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.dao.entity.DataStreamEntity;
import org.apache.inlong.manager.dao.mapper.DataStreamEntityMapper;
import org.apache.inlong.manager.service.core.StorageService;
import org.apache.inlong.manager.service.thirdpart.hive.CreateHiveTableForAllStreamListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreateTubeConsumeGroupTaskEventListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreateTubeTopicTaskEventListener;
import org.apache.inlong.manager.service.thirdpart.sort.PushHiveConfigToSortEventListener;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowDefinition;
import org.apache.inlong.manager.service.workflow.newbusiness.listener.CreateResourceCompleteProcessListener;
import org.apache.inlong.manager.service.workflow.newbusiness.listener.CreateResourceFailedProcessListener;
import org.apache.inlong.manager.service.workflow.newbusiness.listener.InitBusinessInfoListener;
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
public class CreateResourceWorkflowDefinition implements WorkflowDefinition {

    @Autowired
    private InitBusinessInfoListener initBusinessInfoListener;

    @Autowired
    private CreateResourceCompleteProcessListener createResourceCompleteProcessListener;

    @Autowired
    private CreateResourceFailedProcessListener createResourceFailedProcessListener;

    @Autowired
    private CreateTubeTopicTaskEventListener createTubeTopicTaskEventListener;

    @Autowired
    private CreateTubeConsumeGroupTaskEventListener createTubeConsumeGroupTaskEventListener;

    @Autowired
    private CreateHiveTableForAllStreamListener createHiveTableForAllStreamListener;

    @Autowired
    private PushHiveConfigToSortEventListener pushHiveConfigToSortEventListener;

    @Autowired
    private StorageService storageService;

    @Autowired
    private DataStreamEntityMapper streamMapper;

    @Override
    public Process defineProcess() {

        // Configuration process
        Process process = new Process();
        process.addListener(initBusinessInfoListener);
        process.addListener(createResourceCompleteProcessListener);
        process.addListener(createResourceFailedProcessListener);

        process.setType("Business Resource Creation");
        process.setName(getProcessName().name());
        process.setDisplayName(getProcessName().getDisplayName());
        process.setFormClass(CreateResourceWorkflowForm.class);
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
            CreateResourceWorkflowForm form = (CreateResourceWorkflowForm) c.getProcessForm();
            BusinessInfo businessInfo = form.getBusinessInfo();
            if (BizConstant.MIDDLEWARE_TYPE_TUBE.equalsIgnoreCase(businessInfo.getMiddlewareType())) {
                return false;
            }
            log.warn("not need to create tube resource for bid={}, as the middleware type is {}",
                    businessInfo.getMiddlewareType(), form.getBusinessId());
            return true;
        });
        createTubeTopicTask.setName("createTubeTopic");
        createTubeTopicTask.setDisplayName("Create Tube Topic");
        createTubeTopicTask.addListener(createTubeTopicTaskEventListener);
        process.addTask(createTubeTopicTask);

        ServiceTask createConsumerGroupForSortTask = new ServiceTask();
        createConsumerGroupForSortTask.setSkipResolver(c -> {
            CreateResourceWorkflowForm form = (CreateResourceWorkflowForm) c.getProcessForm();
            BusinessInfo businessInfo = form.getBusinessInfo();
            if (BizConstant.MIDDLEWARE_TYPE_TUBE.equalsIgnoreCase(businessInfo.getMiddlewareType())) {
                return false;
            }
            log.warn("no need to create tube resource for bid={}, as the middleware type is {}",
                    form.getBusinessId(), businessInfo.getMiddlewareType());
            return true;
        });
        createConsumerGroupForSortTask.setName("createConsumerGroupForSort");
        createConsumerGroupForSortTask.setDisplayName("Create Consumer Group For Sort");
        createConsumerGroupForSortTask.addListener(createTubeConsumeGroupTaskEventListener);
        process.addTask(createConsumerGroupForSortTask);

        ServiceTask createHiveTablesTask = new ServiceTask();
        createHiveTablesTask.setSkipResolver(c -> {
            CreateResourceWorkflowForm form = (CreateResourceWorkflowForm) c.getProcessForm();
            List<String> dsForHive = storageService
                    .filterStreamIdByStorageType(form.getBusinessId(), BizConstant.STORAGE_TYPE_HIVE,
                            streamMapper
                                    .selectByBid(form.getBusinessId())
                                    .stream()
                                    .map(DataStreamEntity::getDataStreamIdentifier)
                                    .collect(Collectors.toList()));
            if (CollectionUtils.isEmpty(dsForHive)) {
                log.warn("business {} dataStream {} does not have storage, skip to create hive table ",
                        form.getBusinessId(), form.getDataStreamIdentifier());
                return true;
            }
            return false;
        });
        createHiveTablesTask.setName("createHiveTableTask");
        createHiveTablesTask.setDisplayName("Create Hive Table");
        createHiveTablesTask.addListener(createHiveTableForAllStreamListener);
        process.addTask(createHiveTablesTask);

        ServiceTask pushSortConfig = new ServiceTask();
        pushSortConfig.setName("pushSortConfig");
        pushSortConfig.setDisplayName("Push Sort Config");
        pushSortConfig.addListener(pushHiveConfigToSortEventListener);
        process.addTask(pushSortConfig);

        startEvent.addNext(createTubeTopicTask);
        createTubeTopicTask.addNext(createConsumerGroupForSortTask);
        createConsumerGroupForSortTask.addNext(createHiveTablesTask);
        createHiveTablesTask.addNext(pushSortConfig);
        pushSortConfig.addNext(endEvent);

        return process;
    }

    @Override
    public ProcessName getProcessName() {
        return ProcessName.CREATE_BUSINESS_RESOURCE;
    }
}
