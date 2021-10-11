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

package org.apache.inlong.manager.service.workflow.newstream;

import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.service.core.StorageService;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowDefinition;
import org.apache.inlong.manager.service.thirdpart.hive.CreateHiveTableForOneStreamListener;
import org.apache.inlong.manager.service.workflow.newbusiness.CreateResourceWorkflowForm;
import org.apache.inlong.manager.service.workflow.newbusiness.listener.InitBusinessInfoListener;
import org.apache.inlong.manager.service.thirdpart.sort.PushHiveConfigToSortEventListener;
import org.apache.inlong.manager.workflow.model.definition.EndEvent;
import org.apache.inlong.manager.workflow.model.definition.Process;
import org.apache.inlong.manager.workflow.model.definition.ServiceTask;
import org.apache.inlong.manager.workflow.model.definition.StartEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Single data stream access resource creation
 */
@Component
@Slf4j
public class SingleStreamWorkflowDefinition implements WorkflowDefinition {

    @Autowired
    private StorageService storageService;
    @Autowired
    private InitBusinessInfoListener initBusinessInfoListener;
    @Autowired
    private SingleStreamFailedProcessListener singleStreamFailedProcessListener;
    @Autowired
    private SingleStreamCompleteProcessListener singleStreamCompleteProcessListener;
    @Autowired
    private CreateHiveTableForOneStreamListener createHiveTableForOneStreamListener;
    @Autowired
    private PushHiveConfigToSortEventListener pushHiveConfigToSortEventListener;

    @Override
    public Process defineProcess() {
        // Configuration process
        Process process = new Process();
        process.addListener(initBusinessInfoListener);
        process.addListener(singleStreamFailedProcessListener);
        process.addListener(singleStreamCompleteProcessListener);

        process.setType("Data stream access resource creation");
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

        ServiceTask createHiveTableTask = new ServiceTask();
        createHiveTableTask.setSkipResolver(c -> {
            CreateResourceWorkflowForm form = (CreateResourceWorkflowForm) c.getProcessForm();
            String bid = form.getBusinessId();
            String dsid = form.getDataStreamIdentifier();
            List<String> dsForHive = storageService.filterStreamIdByStorageType(bid, BizConstant.STORAGE_TYPE_HIVE,
                    Collections.singletonList(dsid));
            if (CollectionUtils.isEmpty(dsForHive)) {
                log.warn("business [{}] adn data stream [{}] does not have storage, skip create hive table", bid, dsid);
                return true;
            }
            return false;
        });

        createHiveTableTask.setName("createHiveTableTask");
        createHiveTableTask.setDisplayName("Create Hive Table");
        createHiveTableTask.addListener(createHiveTableForOneStreamListener);
        process.addTask(createHiveTableTask);

        ServiceTask pushSortConfig = new ServiceTask();
        pushSortConfig.setName("pushSortConfig");
        pushSortConfig.setDisplayName("Push Sort Configuration");
        pushSortConfig.addListener(pushHiveConfigToSortEventListener);
        process.addTask(pushSortConfig);

        startEvent.addNext(createHiveTableTask);
        createHiveTableTask.addNext(pushSortConfig);
        pushSortConfig.addNext(endEvent);

        return process;
    }

    @Override
    public ProcessName getProcessName() {
        return ProcessName.CREATE_DATASTREAM_RESOURCE;
    }
}
