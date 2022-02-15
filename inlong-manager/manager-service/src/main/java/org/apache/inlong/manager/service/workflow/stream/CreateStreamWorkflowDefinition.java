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

package org.apache.inlong.manager.service.workflow.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.model.definition.EndEvent;
import org.apache.inlong.manager.common.model.definition.Process;
import org.apache.inlong.manager.common.model.definition.ServiceTask;
import org.apache.inlong.manager.common.model.definition.StartEvent;
import org.apache.inlong.manager.service.storage.StorageService;
import org.apache.inlong.manager.service.thirdpart.hive.CreateHiveTableForStreamListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreatePulsarGroupForStreamTaskListener;
import org.apache.inlong.manager.service.thirdpart.mq.CreatePulsarTopicForStreamTaskListener;
import org.apache.inlong.manager.service.thirdpart.sort.PushHiveConfigTaskListener;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowDefinition;
import org.apache.inlong.manager.service.workflow.business.BusinessResourceWorkflowForm;
import org.apache.inlong.manager.service.workflow.business.listener.InitBusinessInfoListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * Data stream access resource creation
 */
@Component
@Slf4j
public class CreateStreamWorkflowDefinition implements WorkflowDefinition {

    @Autowired
    private StorageService storageService;
    @Autowired
    private InitBusinessInfoListener initBusinessInfoListener;
    @Autowired
    private StreamFailedProcessListener streamFailedProcessListener;
    @Autowired
    private StreamCompleteProcessListener streamCompleteProcessListener;
    @Autowired
    private CreateHiveTableForStreamListener createHiveTableListener;
    @Autowired
    private PushHiveConfigTaskListener pushHiveConfigTaskListener;
    @Autowired
    private CreatePulsarTopicForStreamTaskListener createPulsarTopicTaskListener;
    @Autowired
    private CreatePulsarGroupForStreamTaskListener createPulsarGroupTaskListener;

    @Override
    public Process defineProcess() {
        // Configuration process
        Process process = new Process();
        process.addListener(initBusinessInfoListener);
        process.addListener(streamFailedProcessListener);
        process.addListener(streamCompleteProcessListener);

        process.setType("Data stream access resource creation");
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

        ServiceTask createPulsarTopicTask = new ServiceTask();
        createPulsarTopicTask.setSkipResolver(c -> {
            BusinessResourceWorkflowForm form = (BusinessResourceWorkflowForm) c.getProcessForm();
            String middlewareType = form.getBusinessInfo().getMiddlewareType();
            if (BizConstant.MIDDLEWARE_PULSAR.equalsIgnoreCase(middlewareType)) {
                return false;
            }
            log.warn("no need to create pulsar topic for groupId={}, streamId={}, as the middlewareType={}",
                    form.getInlongGroupId(), form.getInlongStreamId(), middlewareType);
            return true;
        });
        createPulsarTopicTask.setName("createPulsarTopic");
        createPulsarTopicTask.setDisplayName("Stream-CreatePulsarTopic");
        createPulsarTopicTask.addListener(createPulsarTopicTaskListener);
        process.addTask(createPulsarTopicTask);

        ServiceTask createPulsarSubscriptionGroupTask = new ServiceTask();
        createPulsarSubscriptionGroupTask.setSkipResolver(c -> {
            BusinessResourceWorkflowForm form = (BusinessResourceWorkflowForm) c.getProcessForm();
            String middlewareType = form.getBusinessInfo().getMiddlewareType();
            if (BizConstant.MIDDLEWARE_PULSAR.equalsIgnoreCase(middlewareType)) {
                return false;
            }
            log.warn("no need to create pulsar subscription for groupId={}, streamId={}, as the middlewareType={}",
                    form.getInlongGroupId(), form.getInlongStreamId(), middlewareType);
            return true;
        });
        createPulsarSubscriptionGroupTask.setName("createPulsarSubscription");
        createPulsarSubscriptionGroupTask.setDisplayName("Stream-CreatePulsarSubscription");
        createPulsarSubscriptionGroupTask.addListener(createPulsarGroupTaskListener);
        process.addTask(createPulsarSubscriptionGroupTask);

        ServiceTask createHiveTableTask = new ServiceTask();
        createHiveTableTask.setSkipResolver(c -> {
            BusinessResourceWorkflowForm form = (BusinessResourceWorkflowForm) c.getProcessForm();
            String groupId = form.getInlongGroupId();
            String streamId = form.getInlongStreamId();
            List<String> dsForHive = storageService.getExistsStreamIdList(groupId, BizConstant.STORAGE_HIVE,
                    Collections.singletonList(streamId));
            if (CollectionUtils.isEmpty(dsForHive)) {
                log.warn("business [{}] adn data stream [{}] does not have storage, skip create hive table", groupId,
                        streamId);
                return true;
            }
            return false;
        });

        createHiveTableTask.setName("createHiveTable");
        createHiveTableTask.setDisplayName("Stream-CreateHiveTable");
        createHiveTableTask.addListener(createHiveTableListener);
        process.addTask(createHiveTableTask);

        ServiceTask pushSortConfig = new ServiceTask();
        pushSortConfig.setName("pushSortConfig");
        pushSortConfig.setDisplayName("Stream-PushSortConfig");
        pushSortConfig.addListener(pushHiveConfigTaskListener);
        process.addTask(pushSortConfig);

        startEvent.addNext(createPulsarTopicTask);
        createPulsarTopicTask.addNext(createPulsarSubscriptionGroupTask);
        createPulsarSubscriptionGroupTask.addNext(createHiveTableTask);
        createHiveTableTask.addNext(pushSortConfig);
        pushSortConfig.addNext(endEvent);

        return process;
    }

    @Override
    public ProcessName getProcessName() {
        return ProcessName.CREATE_DATASTREAM_RESOURCE;
    }
}
