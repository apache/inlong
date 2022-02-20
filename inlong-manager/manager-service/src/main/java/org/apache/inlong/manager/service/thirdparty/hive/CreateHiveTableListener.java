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

package org.apache.inlong.manager.service.thirdparty.hive;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.pojo.sink.SinkForSortDTO;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SinkOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Event listener of create hive table for all inlong stream
 */
@Service
@Slf4j
public class CreateHiveTableListener implements SinkOperateListener {

    @Autowired
    private StreamSinkEntityMapper sinkMapper;
    @Autowired
    private HiveTableOperator hiveTableOperator;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) {
        GroupResourceProcessForm form = (GroupResourceProcessForm) context.getProcessForm();
        String groupId = form.getInlongGroupId();
        log.info("begin to create hive table for groupId={}", groupId);

        List<SinkForSortDTO> configList = sinkMapper.selectAllConfig(groupId, null);
        List<SinkForSortDTO> needCreateList = configList.stream()
                .filter(sinkForSortDTO -> sinkForSortDTO.getEnableCreateResource() == 1)
                .collect(Collectors.toList());
        hiveTableOperator.createHiveResource(groupId, needCreateList);

        String result = "success to create hive table for group [" + groupId + "]";
        log.info(result);
        return ListenerResult.success(result);
    }

    @Override
    public boolean async() {
        return false;
    }

}
