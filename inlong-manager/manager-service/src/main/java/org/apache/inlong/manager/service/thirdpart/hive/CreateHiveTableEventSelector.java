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
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.event.EventSelector;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.definition.ProcessForm;
import org.apache.inlong.manager.dao.entity.DataStreamEntity;
import org.apache.inlong.manager.dao.mapper.DataStreamEntityMapper;
import org.apache.inlong.manager.service.core.StorageService;
import org.apache.inlong.manager.service.workflow.business.BusinessResourceWorkflowForm;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CreateHiveTableEventSelector implements EventSelector {

    @Autowired
    private StorageService storageService;
    @Autowired
    private DataStreamEntityMapper streamMapper;

    @Override
    public boolean accept(WorkflowContext context) {
        ProcessForm processForm = context.getProcessForm();
        if (processForm == null || !(processForm instanceof BusinessResourceWorkflowForm)) {
            return false;
        }
        BusinessResourceWorkflowForm form = (BusinessResourceWorkflowForm) processForm;
        if (form.getBusinessInfo() == null || StringUtils.isEmpty(form.getBusinessInfo().getInlongGroupId())) {
            return false;
        }
        String groupId = form.getInlongGroupId();
        List<String> dsForHive = storageService.filterStreamIdByStorageType(groupId, BizConstant.STORAGE_HIVE,
                streamMapper.selectByGroupId(groupId)
                        .stream()
                        .map(DataStreamEntity::getInlongStreamId)
                        .collect(Collectors.toList()));
        //todo check if create hive automatically
        if (CollectionUtils.isEmpty(dsForHive)) {
            log.warn("groupId={} streamId={} does not have storage, skip to create hive table ",
                    groupId, form.getInlongStreamId());
            return true;
        }
        return false;
    }
}
