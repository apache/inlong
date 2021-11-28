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

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.pojo.datastorage.StorageHiveSortInfo;
import org.apache.inlong.manager.dao.mapper.StorageHiveEntityMapper;
import org.apache.inlong.manager.service.workflow.newbusiness.CreateResourceWorkflowForm;
import org.apache.inlong.manager.workflow.core.event.ListenerResult;
import org.apache.inlong.manager.workflow.core.event.task.TaskEvent;
import org.apache.inlong.manager.workflow.core.event.task.TaskEventListener;
import org.apache.inlong.manager.workflow.model.WorkflowContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Event listener of create hive table for one data stream
 */
@Service
@Slf4j
public class CreateHiveTableForOneStreamListener implements TaskEventListener {

    @Autowired
    private StorageHiveEntityMapper hiveEntityMapper;
    @Autowired
    private HiveTableOperator hiveTableOperator;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) {
        CreateResourceWorkflowForm form = (CreateResourceWorkflowForm) context.getProcessForm();
        String groupId = form.getInlongGroupId();
        String streamId = form.getInlongStreamId();
        log.info("begin create hive table for groupId={}, streamId={}", groupId, streamId);

        List<StorageHiveSortInfo> hiveConfig = hiveEntityMapper.selectHiveSortInfoByIdentifier(groupId, streamId);
        if (hiveConfig == null || hiveConfig.size() == 0) {
            return ListenerResult.success();
        }
        for (StorageHiveSortInfo info : hiveConfig) {
            hiveTableOperator.createHiveTable(groupId, info);
        }

        log.info("finish create hive table for business {} - {} ", groupId, streamId);
        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return false;
    }

}
