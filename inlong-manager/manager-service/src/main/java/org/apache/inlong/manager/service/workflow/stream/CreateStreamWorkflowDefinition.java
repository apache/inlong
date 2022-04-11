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
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.thirdparty.hive.CreateHiveTableForStreamListener;
import org.apache.inlong.manager.service.thirdparty.mq.CreatePulsarGroupForStreamTaskListener;
import org.apache.inlong.manager.service.thirdparty.mq.CreatePulsarTopicForStreamTaskListener;
import org.apache.inlong.manager.service.thirdparty.sort.PushSortConfigListener;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowDefinition;
import org.apache.inlong.manager.service.workflow.group.listener.InitGroupListener;
import org.apache.inlong.manager.workflow.definition.EndEvent;
import org.apache.inlong.manager.workflow.definition.ServiceTask;
import org.apache.inlong.manager.workflow.definition.StartEvent;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * Inlong stream access resource creation
 */
@Component
@Slf4j
public class CreateStreamWorkflowDefinition implements WorkflowDefinition {

    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InitGroupListener initGroupListener;
    @Autowired
    private StreamFailedProcessListener streamFailedProcessListener;
    @Autowired
    private StreamCompleteProcessListener streamCompleteProcessListener;
    @Autowired
    private CreateHiveTableForStreamListener createHiveTableListener;
    @Autowired
    private PushSortConfigListener pushSortConfigListener;
    @Autowired
    private CreatePulsarTopicForStreamTaskListener createPulsarTopicTaskListener;
    @Autowired
    private CreatePulsarGroupForStreamTaskListener createPulsarGroupTaskListener;

    @Override
    public WorkflowProcess defineProcess() {
        // Configuration process
        WorkflowProcess process = new WorkflowProcess();
        process.addListener(initGroupListener);
        process.addListener(streamFailedProcessListener);
        process.addListener(streamCompleteProcessListener);

        process.setType("Inlong stream access resource creation");
        process.setName(getProcessName().name());
        process.setDisplayName(getProcessName().getDisplayName());
        process.setFormClass(GroupResourceProcessForm.class);
        process.setVersion(1);
        process.setHidden(1);

        // Start node
        StartEvent startEvent = new StartEvent();
        process.setStartEvent(startEvent);

        // End node
        EndEvent endEvent = new EndEvent();
        process.setEndEvent(endEvent);

        ServiceTask createPulsarTopicTask = new ServiceTask();
        createPulsarTopicTask.setSkipResolver(c -> {
            GroupResourceProcessForm form = (GroupResourceProcessForm) c.getProcessForm();
            MQType mqType = MQType.forType(form.getGroupInfo().getMiddlewareType());
            if (mqType == MQType.PULSAR || mqType == MQType.TDMQ_PULSAR) {
                return false;
            }
            log.warn("no need to create pulsar topic for groupId={}, streamId={}, as the middlewareType={}",
                    form.getInlongGroupId(), form.getInlongStreamId(), mqType);
            return true;
        });
        createPulsarTopicTask.setName("createPulsarTopic");
        createPulsarTopicTask.setDisplayName("Stream-CreatePulsarTopic");
        createPulsarTopicTask.addListener(createPulsarTopicTaskListener);
        process.addTask(createPulsarTopicTask);

        ServiceTask createPulsarSubscriptionGroupTask = new ServiceTask();
        createPulsarSubscriptionGroupTask.setSkipResolver(c -> {
            GroupResourceProcessForm form = (GroupResourceProcessForm) c.getProcessForm();
            MQType mqType = MQType.forType(form.getGroupInfo().getMiddlewareType());
            if (mqType == MQType.PULSAR || mqType == MQType.TDMQ_PULSAR) {
                return false;
            }
            log.warn("no need to create pulsar subscription for groupId={}, streamId={}, as the middlewareType={}",
                    form.getInlongGroupId(), form.getInlongStreamId(), mqType);
            return true;
        });
        createPulsarSubscriptionGroupTask.setName("createPulsarSubscription");
        createPulsarSubscriptionGroupTask.setDisplayName("Stream-CreatePulsarSubscription");
        createPulsarSubscriptionGroupTask.addListener(createPulsarGroupTaskListener);
        process.addTask(createPulsarSubscriptionGroupTask);

        ServiceTask createHiveTableTask = new ServiceTask();
        createHiveTableTask.setSkipResolver(c -> {
            GroupResourceProcessForm form = (GroupResourceProcessForm) c.getProcessForm();
            String groupId = form.getInlongGroupId();
            String streamId = form.getInlongStreamId();
            List<String> dsForHive = sinkService.getExistsStreamIdList(groupId, SinkType.SINK_HIVE,
                    Collections.singletonList(streamId));
            if (CollectionUtils.isEmpty(dsForHive)) {
                log.warn("inlong group [{}] adn inlong stream [{}] does not have sink, skip create hive table", groupId,
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
        pushSortConfig.addListener(pushSortConfigListener);
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
        return ProcessName.CREATE_STREAM_RESOURCE;
    }

}
