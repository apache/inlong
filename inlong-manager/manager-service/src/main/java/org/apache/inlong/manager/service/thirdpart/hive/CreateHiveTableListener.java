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

package org.apache.inlong.manager.service.thirdpart.hive;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.event.ListenerResult;
import org.apache.inlong.manager.common.event.task.StorageOperateListener;
import org.apache.inlong.manager.common.event.task.TaskEvent;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.pojo.datastorage.StorageForSortDTO;
import org.apache.inlong.manager.dao.mapper.StorageEntityMapper;
import org.apache.inlong.manager.service.workflow.business.BusinessResourceWorkflowForm;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Event listener of create hive table for all data stream
 */
@Service
@Slf4j
public class CreateHiveTableListener implements StorageOperateListener {

    @Autowired
    private StorageEntityMapper storageMapper;
    @Autowired
    private HiveTableOperator hiveTableOperator;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) {
        BusinessResourceWorkflowForm form = (BusinessResourceWorkflowForm) context.getProcessForm();
        String groupId = form.getInlongGroupId();
        log.info("begin to create hive table for groupId={}", groupId);

        List<StorageForSortDTO> configList = storageMapper.selectAllConfig(groupId, null);
        hiveTableOperator.createHiveResource(groupId, configList);

        String result = "success to create hive table for group [" + groupId + "]";
        log.info(result);
        return ListenerResult.success(result);
    }

    @Override
    public boolean async() {
        return false;
    }
}
