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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongGroupConf;
import org.apache.inlong.manager.client.api.InlongGroupInfo;
import org.apache.inlong.manager.client.api.InlongGroupInfo.InlongGroupState;
import org.apache.inlong.manager.client.api.InlongStream;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.api.InlongStreamConf;
import org.apache.inlong.manager.client.api.inner.InnerGroupContext;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.client.api.util.AssertUtil;
import org.apache.inlong.manager.client.api.util.GsonUtil;
import org.apache.inlong.manager.client.api.util.InlongGroupTransfer;
import org.apache.inlong.manager.client.api.util.InlongParser;
import org.apache.inlong.manager.common.enums.GroupState;
import org.apache.inlong.manager.common.enums.ProcessStatus;
import org.apache.inlong.manager.common.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamApproveRequest;
import org.apache.inlong.manager.common.pojo.workflow.ProcessResponse;
import org.apache.inlong.manager.common.pojo.workflow.TaskResponse;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;

public class InlongGroupImpl implements InlongGroup {

    private InlongGroupConf groupConf;

    private InnerInlongManagerClient managerClient;

    private InnerGroupContext groupContext;

    public InlongGroupImpl(InlongGroupConf groupConf, InlongClientImpl inlongClient) {
        this.groupConf = groupConf;
        this.groupContext = new InnerGroupContext();
        InlongGroupRequest groupRequest = InlongGroupTransfer.createGroupInfo(groupConf);
        this.groupContext.setGroupRequest(groupRequest);
        if (this.managerClient == null) {
            this.managerClient = new InnerInlongManagerClient(inlongClient);
        }
        Pair<Boolean, InlongGroupRequest> existMsg = managerClient.isGroupExists(groupRequest);
        if (existMsg.getKey()) {
            //Update current snapshot
            this.groupContext.setGroupRequest(existMsg.getValue());
        } else {
            String groupId = managerClient.createGroupInfo(groupRequest);
            groupRequest.setInlongGroupId(groupId);
        }
    }

    @Override
    public InlongStreamBuilder createStream(InlongStreamConf dataStreamConf) throws Exception {
        return new DefaultInlongStreamBuilder(dataStreamConf, this.groupContext, this.managerClient) {
        };
    }

    @Override
    public InlongGroupInfo init() throws Exception {
        WorkflowResult initWorkflowResult = managerClient.initInlongGroup(this.groupContext.getGroupRequest());
        List<TaskResponse> taskViews = initWorkflowResult.getNewTasks();
        AssertUtil.notEmpty(taskViews, "Init business info failed");
        TaskResponse taskView = taskViews.get(0);
        final int taskId = taskView.getId();
        ProcessResponse processView = initWorkflowResult.getProcessInfo();
        AssertUtil.isTrue(ProcessStatus.PROCESSING == processView.getStatus(),
                String.format("Business info state : %s is not corrected , should be PROCESSING",
                        processView.getStatus()));
        String formData = GsonUtil.toJson(processView.getFormData());
        Pair<InlongGroupApproveRequest, List<InlongStreamApproveRequest>> initMsg = InlongParser
                .parseGroupForm(formData);
        groupContext.setInitMsg(initMsg);
        WorkflowResult startWorkflowResult = managerClient.startInlongGroup(taskId, initMsg);
        processView = startWorkflowResult.getProcessInfo();
        AssertUtil.isTrue(ProcessStatus.COMPLETED == processView.getStatus(),
                String.format("Business info state : %s is not corrected , should be COMPLETED",
                        processView.getStatus()));
        return generateSnapshot(null);
    }

    @Override
    public InlongGroupInfo suspend() throws Exception {
        Pair<String, String> idAndErr = managerClient.updateGroupInfo(groupContext.getGroupRequest());
        final String errMsg = idAndErr.getValue();
        final String groupId = idAndErr.getKey();
        AssertUtil.isNull(errMsg, errMsg);
        managerClient.operateInlongGroup(groupId, InlongGroupState.SUSPEND);
        return generateSnapshot(null);
    }

    @Override
    public InlongGroupInfo restart() throws Exception {
        Pair<String, String> idAndErr = managerClient.updateGroupInfo(groupContext.getGroupRequest());
        final String errMsg = idAndErr.getValue();
        final String groupId = idAndErr.getKey();
        AssertUtil.isNull(errMsg, errMsg);
        managerClient.operateInlongGroup(groupId, InlongGroupState.RESTART);
        return generateSnapshot(null);
    }

    @Override
    public InlongGroupInfo delete() throws Exception {
        InlongGroupRequest curGroupRequest = managerClient.getGroupInfo(
                groupContext.getGroupRequest().getInlongGroupId());
        boolean isDeleted = managerClient.deleteInlongGroup(curGroupRequest.getInlongGroupId());
        if (isDeleted) {
            curGroupRequest.setStatus(GroupState.GROUP_DELETE.getCode());
        }
        return generateSnapshot(curGroupRequest);
    }

    @Override
    public List<InlongStream> listStreams() throws Exception {
        String inlongGroupId = this.groupContext.getGroupId();
        return fetchDataStreams(inlongGroupId);
    }

    private InlongGroupInfo generateSnapshot(InlongGroupRequest currentBizInfo) {
        if (currentBizInfo == null) {
            currentBizInfo = managerClient.getGroupInfo(
                    groupContext.getGroupRequest().getInlongGroupId());
            groupContext.setGroupRequest(currentBizInfo);
        }
        String inlongGroupId = currentBizInfo.getInlongGroupId();
        List<InlongStream> dataStreams = fetchDataStreams(inlongGroupId);
        dataStreams.stream().forEach(dataStream -> groupContext.setStream(dataStream));
        return new InlongGroupInfo(groupContext, groupConf);
    }

    private List<InlongStream> fetchDataStreams(String groupId) {
        List<FullStreamResponse> streamResponses = managerClient.listStreamInfo(groupId);
        List<InlongStream> streamList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(streamResponses)) {
            streamList = streamResponses.stream().map(fullStreamResponse -> {
                String streamName = fullStreamResponse.getStreamInfo().getName();
                InlongStream stream = groupContext.getStream(streamName);
                return new InlongStreamImpl(fullStreamResponse, stream);
            }).collect(Collectors.toList());
        }
        return streamList;
    }
}
