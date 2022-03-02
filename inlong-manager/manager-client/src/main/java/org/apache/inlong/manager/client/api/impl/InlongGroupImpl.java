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
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongGroupConf;
import org.apache.inlong.manager.client.api.GroupInfo;
import org.apache.inlong.manager.client.api.GroupInfo.InlongGroupState;
import org.apache.inlong.manager.client.api.InlongStream;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.api.InlongStreamConf;
import org.apache.inlong.manager.client.api.inner.InnerGroupContext;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.client.api.util.AssertUtil;
import org.apache.inlong.manager.client.api.util.GsonUtil;
import org.apache.inlong.manager.client.api.util.InlongGroupTransfer;
import org.apache.inlong.manager.client.api.util.InlongParser;
import org.apache.inlong.manager.client.api.util.InlongStreamSourceTransfer;
import org.apache.inlong.manager.common.enums.GroupState;
import org.apache.inlong.manager.common.enums.ProcessStatus;
import org.apache.inlong.manager.common.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupResponse;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamApproveRequest;
import org.apache.inlong.manager.common.pojo.workflow.EventLogView;
import org.apache.inlong.manager.common.pojo.workflow.ProcessResponse;
import org.apache.inlong.manager.common.pojo.workflow.TaskResponse;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.common.util.CommonBeanUtils;

public class InlongGroupImpl implements InlongGroup {

    private InlongGroupConf groupConf;

    private InnerInlongManagerClient managerClient;

    private InnerGroupContext groupContext;

    public InlongGroupImpl(InlongGroupConf groupConf, InlongClientImpl inlongClient) {
        this.groupConf = groupConf;
        this.groupContext = new InnerGroupContext();
        InlongGroupInfo groupInfo = InlongGroupTransfer.createGroupInfo(groupConf);
        this.groupContext.setGroupInfo(groupInfo);
        if (this.managerClient == null) {
            this.managerClient = new InnerInlongManagerClient(inlongClient);
        }
        InlongGroupRequest inlongGroupRequest = groupInfo.genRequest();
        Pair<Boolean, InlongGroupResponse> existMsg = managerClient.isGroupExists(inlongGroupRequest);
        if (existMsg.getKey()) {
            //Update current snapshot
            groupInfo = CommonBeanUtils.copyProperties(existMsg.getValue(), InlongGroupInfo::new);
            this.groupContext.setGroupInfo(groupInfo);
        } else {
            String groupId = managerClient.createGroup(inlongGroupRequest);
            groupInfo.setInlongGroupId(groupId);
        }
    }

    @Override
    public InlongStreamBuilder createStream(InlongStreamConf dataStreamConf) throws Exception {
        return new DefaultInlongStreamBuilder(dataStreamConf, this.groupContext, this.managerClient);
    }

    @Override
    public GroupInfo snapshot() throws Exception {
        return generateSnapshot(groupContext.getGroupInfo());
    }

    @Override
    public GroupInfo init() throws Exception {
        InlongGroupInfo groupInfo = this.groupContext.getGroupInfo();
        WorkflowResult initWorkflowResult = managerClient.initInlongGroup(groupInfo.genRequest());
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
    public GroupInfo initOnUpdate(InlongGroupConf conf) throws Exception {
        if (conf != null) {
            AssertUtil.isTrue(conf.getGroupName() != null
                            && conf.getGroupName().equals(this.groupConf.getGroupName()),
                    "Group must have same name");
            this.groupConf = conf;
        } else {
            conf = this.groupConf;
        }
        InlongGroupInfo groupInfo = InlongGroupTransfer.createGroupInfo(conf);
        InlongGroupRequest groupRequest = groupInfo.genRequest();
        Pair<String, String> idAndErr = managerClient.updateGroup(groupRequest);
        String errMsg = idAndErr.getValue();
        AssertUtil.isNull(errMsg, errMsg);
        Pair<Boolean, InlongGroupResponse> existMsg = managerClient.isGroupExists(groupRequest);
        if (existMsg.getKey()) {
            groupInfo = CommonBeanUtils.copyProperties(existMsg.getValue(), InlongGroupInfo::new);
            this.groupContext.setGroupInfo(groupInfo);
            return init();
        } else {
            throw new RuntimeException(String.format("Group is not found by groupName=%s", groupInfo.getName()));
        }
    }

    @Override
    public GroupInfo suspend() throws Exception {
        InlongGroupInfo groupInfo = groupContext.getGroupInfo();
        Pair<String, String> idAndErr = managerClient.updateGroup(groupInfo.genRequest());
        final String errMsg = idAndErr.getValue();
        final String groupId = idAndErr.getKey();
        AssertUtil.isNull(errMsg, errMsg);
        managerClient.operateInlongGroup(groupId, InlongGroupState.STOPPED);
        return generateSnapshot(null);
    }

    @Override
    public GroupInfo restart() throws Exception {
        InlongGroupInfo groupInfo = groupContext.getGroupInfo();
        Pair<String, String> idAndErr = managerClient.updateGroup(groupInfo.genRequest());
        final String errMsg = idAndErr.getValue();
        final String groupId = idAndErr.getKey();
        AssertUtil.isNull(errMsg, errMsg);
        managerClient.operateInlongGroup(groupId, InlongGroupState.STARTED);
        return generateSnapshot(null);
    }

    @Override
    public GroupInfo delete() throws Exception {
        InlongGroupResponse groupResponse = managerClient.getGroupInfo(
                groupContext.getGroupId());
        boolean isDeleted = managerClient.deleteInlongGroup(groupResponse.getInlongGroupId());
        if (isDeleted) {
            groupResponse.setStatus(GroupState.GROUP_DELETE.getCode());
        }
        InlongGroupInfo groupInfo = CommonBeanUtils.copyProperties(groupResponse, InlongGroupInfo::new);
        return generateSnapshot(groupInfo);
    }

    @Override
    public List<InlongStream> listStreams() throws Exception {
        String inlongGroupId = this.groupContext.getGroupId();
        return fetchDataStreams(inlongGroupId);
    }

    private GroupInfo generateSnapshot(InlongGroupInfo currentGroupInfo) {
        if (currentGroupInfo == null) {
            InlongGroupResponse groupResponse = managerClient.getGroupInfo(
                    groupContext.getGroupId());
            currentGroupInfo = CommonBeanUtils.copyProperties(groupResponse, InlongGroupInfo::new);
            groupContext.setGroupInfo(currentGroupInfo);
        }
        String inlongGroupId = currentGroupInfo.getInlongGroupId();
        List<InlongStream> dataStreams = fetchDataStreams(inlongGroupId);
        dataStreams.stream().forEach(dataStream -> groupContext.setStream(dataStream));
        GroupInfo groupInfo = new GroupInfo(groupContext, groupConf);
        List<EventLogView> logViews = managerClient.getInlongGroupError(inlongGroupId);
        Map<String, String> errMsgs = logViews.stream().collect(
                Collectors.toMap(EventLogView::getEvent, EventLogView::getException));
        groupInfo.setErrMsg(errMsgs);
        return groupInfo;
    }

    private List<InlongStream> fetchDataStreams(String groupId) {
        List<FullStreamResponse> streamResponses = managerClient.listStreamInfo(groupId);
        List<InlongStream> streamList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(streamResponses)) {
            streamList = streamResponses.stream().map(fullStreamResponse -> {
                List<SourceListResponse> sourceListResponses = managerClient.listSources(groupId,
                        fullStreamResponse.getStreamInfo().getInlongStreamId());
                String streamName = fullStreamResponse.getStreamInfo().getName();
                InlongStream stream = groupContext.getStream(streamName);
                InlongStreamImpl inlongStream = new InlongStreamImpl(fullStreamResponse, stream);
                if (CollectionUtils.isNotEmpty(sourceListResponses)) {
                    inlongStream.setStreamSource(
                            InlongStreamSourceTransfer.parseStreamSource(sourceListResponses.get(0)));
                }
                return inlongStream;
            }).collect(Collectors.toList());
        }
        return streamList;
    }
}
