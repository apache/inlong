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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.ProcessForm;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.EventSelector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
public class CreateHiveTableEventSelector implements EventSelector {

    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InlongStreamEntityMapper streamMapper;

    @Override
    public boolean accept(WorkflowContext context) {
        ProcessForm processForm = context.getProcessForm();
        if (!(processForm instanceof GroupResourceProcessForm)) {
            return false;
        }
        GroupResourceProcessForm form = (GroupResourceProcessForm) processForm;
        if (form.getGroupInfo() == null || StringUtils.isEmpty(form.getGroupInfo().getInlongGroupId())) {
            return false;
        }
        String groupId = form.getInlongGroupId();
        List<String> dsForHive = sinkService.getExistsStreamIdList(groupId, SinkType.SINK_HIVE,
                streamMapper.selectByGroupId(groupId)
                        .stream()
                        .map(InlongStreamEntity::getInlongStreamId)
                        .collect(Collectors.toList()));

        if (CollectionUtils.isEmpty(dsForHive)) {
            log.warn("groupId={} streamId={} does not have sink, skip to create hive table ",
                    groupId, form.getInlongStreamId());
            return true;
        }
        return false;
    }

}
