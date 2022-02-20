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

package org.apache.inlong.manager.client.api.impl;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongGroupConf;
import org.apache.inlong.manager.client.api.InlongGroupInfo;
import org.apache.inlong.manager.client.api.InlongGroupInfo.GroupState;
import org.apache.inlong.manager.client.api.InlongStream;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.api.InlongStreamConf;
import org.apache.inlong.manager.client.api.inner.InnerGroupContext;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.client.api.util.AssertUtil;
import org.apache.inlong.manager.client.api.util.InlongGroupTransfer;
import org.apache.inlong.manager.client.api.util.InlongParser;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.enums.ProcessStatus;
import org.apache.inlong.manager.common.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamApproveRequest;
import org.apache.inlong.manager.common.pojo.workflow.ProcessResponse;
import org.apache.inlong.manager.common.pojo.workflow.TaskResponse;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class InlongGroupImpl implements InlongGroup {

    private final InlongGroupConf groupConf;
    private final InnerGroupContext groupContext;
    private InnerInlongManagerClient managerClient;

    public InlongGroupImpl(InlongGroupConf groupConf, InlongClientImpl inlongClient) {
        this.groupConf = groupConf;
        this.groupContext = new InnerGroupContext();
        InlongGroupRequest groupInfo = InlongGroupTransfer.createGroupInfo(groupConf);
        this.groupContext.setGroupInfo(groupInfo);
        if (this.managerClient == null) {
            this.managerClient = new InnerInlongManagerClient(inlongClient);
        }
        Pair<Boolean, InlongGroupRequest> existMsg = managerClient.isGroupExists(groupInfo);
        if (existMsg.getKey()) {
            // Update current snapshot
            this.groupContext.setGroupInfo(existMsg.getValue());
        } else {
            String groupId = managerClient.createGroupInfo(groupInfo);
            groupInfo.setInlongGroupId(groupId);
        }
    }

    @Override
    public InlongStreamBuilder createStream(InlongStreamConf streamConf) {
        return new DefaultInlongStreamBuilder(streamConf, this.groupContext, this.managerClient);
    }

    @Override
    public InlongGroupInfo init() throws Exception {
        WorkflowResult initWorkflowResult = managerClient.initInlongGroup(this.groupContext.getGroupInfo());
        List<TaskResponse> tasks = initWorkflowResult.getNewTasks();
        AssertUtil.notEmpty(tasks, "Init inlong group failed");
        TaskResponse task = tasks.get(0);
        final int taskId = task.getId();

        ProcessResponse response = initWorkflowResult.getProcessInfo();
        AssertUtil.isTrue(ProcessStatus.PROCESSING == response.getStatus(),
                String.format("Process status: %s is not corrected, should be PROCESSING", response.getStatus()));
        String formData = response.getFormData().toString();
        Pair<InlongGroupApproveRequest, List<InlongStreamApproveRequest>> initMsg = InlongParser.parseGroupForm(
                formData);
        groupContext.setInitMsg(initMsg);
        WorkflowResult startWorkflowResult = managerClient.startInlongGroup(taskId, initMsg);
        response = startWorkflowResult.getProcessInfo();
        AssertUtil.isTrue(ProcessStatus.COMPLETED == response.getStatus(),
                String.format("Inlong group status: %s is not corrected, should be COMPLETED", response.getStatus()));
        return generateSnapshot(null);
    }

    @Override
    public InlongGroupInfo suspend() {
        Pair<String, String> idAndErr = managerClient.updateGroupInfo(groupContext.getGroupInfo());
        final String errMsg = idAndErr.getValue();
        final String groupId = idAndErr.getKey();
        AssertUtil.isNull(errMsg, errMsg);
        managerClient.operateInlongGroup(groupId, GroupState.SUSPEND);
        return generateSnapshot(null);
    }

    @Override
    public InlongGroupInfo restart() {
        Pair<String, String> idAndErr = managerClient.updateGroupInfo(groupContext.getGroupInfo());
        final String errMsg = idAndErr.getValue();
        final String groupId = idAndErr.getKey();
        AssertUtil.isNull(errMsg, errMsg);
        managerClient.operateInlongGroup(groupId, GroupState.RESTART);
        return generateSnapshot(null);
    }

    @Override
    public InlongGroupInfo delete() {
        InlongGroupRequest groupInfo = managerClient.getGroupInfo(groupContext.getGroupInfo().getInlongGroupId());
        boolean isDeleted = managerClient.deleteInlongGroup(groupInfo.getInlongGroupId());
        if (isDeleted) {
            groupInfo.setStatus(EntityStatus.DELETED.getCode());
        }
        return generateSnapshot(groupInfo);
    }

    @Override
    public List<InlongStream> listStreams() {
        String inlongGroupId = this.groupContext.getGroupId();
        return fetchInlongStreams(inlongGroupId);
    }

    private InlongGroupInfo generateSnapshot(InlongGroupRequest currentBizInfo) {
        if (currentBizInfo == null) {
            currentBizInfo = managerClient.getGroupInfo(groupContext.getGroupInfo().getInlongGroupId());
        }
        String inlongGroupId = currentBizInfo.getInlongGroupId();
        List<InlongStream> streamList = fetchInlongStreams(inlongGroupId);
        streamList.forEach(groupContext::setStream);
        return new InlongGroupInfo(groupContext, groupConf);
    }

    private List<InlongStream> fetchInlongStreams(String groupId) {
        List<FullStreamResponse> streamResponses = managerClient.listStreamInfo(groupId);
        List<InlongStream> streamList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(streamResponses)) {
            streamList = streamResponses.stream().map(InlongStreamImpl::new).collect(Collectors.toList());
        }
        return streamList;
    }

}
