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

package org.apache.inlong.manager.service.thirdparty.sort;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.ProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm.OperateType;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.service.CommonOperateService;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Create sort config when disable the ZooKeeper
 */
@Component
public class CreateSortConfigListener implements SortOperateListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateSortConfigListener.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // thread safe

    @Autowired
    private CommonOperateService commonOperateService;
    @Autowired
    private StreamSinkService streamSinkService;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        LOGGER.info("Create sort config for context={}", context);
        ProcessForm form = context.getProcessForm();
        if (form instanceof UpdateGroupProcessForm) {
            UpdateGroupProcessForm updateGroupProcessForm = (UpdateGroupProcessForm) form;
            OperateType operateType = updateGroupProcessForm.getOperateType();
            if (operateType == OperateType.SUSPEND || operateType == OperateType.DELETE) {
                return ListenerResult.success();
            }
        }
        InlongGroupInfo groupInfo = this.getGroupInfo(form);
        String groupId = groupInfo.getInlongGroupId();
        if (StringUtils.isEmpty(groupId)) {
            LOGGER.warn("GroupId is null for context={}", context);
            return ListenerResult.success();
        }

        List<SinkResponse> sinkResponseList = streamSinkService.listSink(groupId, null);
        if (CollectionUtils.isEmpty(sinkResponseList)) {
            LOGGER.warn("Sink not found by groupId={}", groupId);
            return ListenerResult.success();
        }

        try {
            Map<String, DataFlowInfo> dataFlowInfoMap = sinkResponseList.stream().map(sink -> {
                        DataFlowInfo flowInfo = commonOperateService.createDataFlow(groupInfo, sink);
                        return Pair.of(sink.getInlongStreamId(), flowInfo);
                    }
            ).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

            String dataFlows = OBJECT_MAPPER.writeValueAsString(dataFlowInfoMap);
            InlongGroupExtInfo extInfo = new InlongGroupExtInfo();
            extInfo.setInlongGroupId(groupId);
            extInfo.setKeyName(InlongGroupSettings.DATA_FLOW);
            extInfo.setKeyValue(dataFlows);
            if (groupInfo.getExtList() == null) {
                groupInfo.setExtList(Lists.newArrayList());
            }
            upsertDataFlow(groupInfo, extInfo);
        } catch (Exception e) {
            LOGGER.error("create sort config failed for sink list={} ", sinkResponseList, e);
            throw new WorkflowListenerException("create sort config failed: " + e.getMessage());
        }

        return ListenerResult.success();
    }

    private void upsertDataFlow(InlongGroupInfo groupInfo, InlongGroupExtInfo extInfo) {
        groupInfo.getExtList().removeIf(ext -> InlongGroupSettings.DATA_FLOW.equals(ext.getKeyName()));
        groupInfo.getExtList().add(extInfo);
    }

    private InlongGroupInfo getGroupInfo(ProcessForm processForm) {
        if (processForm instanceof GroupResourceProcessForm) {
            GroupResourceProcessForm groupResourceProcessForm = (GroupResourceProcessForm) processForm;
            return groupResourceProcessForm.getGroupInfo();
        } else if (processForm instanceof UpdateGroupProcessForm) {
            UpdateGroupProcessForm updateGroupProcessForm = (UpdateGroupProcessForm) processForm;
            return updateGroupProcessForm.getGroupInfo();
        } else {
            LOGGER.error("Illegal ProcessForm {} to get inlong group info", processForm.getFormName());
            throw new WorkflowListenerException(
                    String.format("Unsupported ProcessForm {%s}", processForm.getFormName()));
        }
    }

    @Override
    public boolean async() {
        return false;
    }
}
