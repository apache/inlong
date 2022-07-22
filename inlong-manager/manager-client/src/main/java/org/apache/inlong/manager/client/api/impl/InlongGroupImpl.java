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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongGroupContext;
import org.apache.inlong.manager.client.api.InlongStream;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.api.enums.SimpleGroupStatus;
import org.apache.inlong.manager.client.api.inner.InnerGroupContext;
import org.apache.inlong.manager.client.api.inner.client.ClientFactory;
import org.apache.inlong.manager.client.api.inner.client.InlongGroupClient;
import org.apache.inlong.manager.client.api.inner.client.InlongStreamClient;
import org.apache.inlong.manager.client.api.inner.client.WorkflowClient;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.client.api.util.InlongGroupTransfer;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.enums.ProcessStatus;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupResetRequest;
import org.apache.inlong.manager.common.pojo.sort.BaseSortConf;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamConfigLogListResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.workflow.EventLogView;
import org.apache.inlong.manager.common.pojo.workflow.ProcessResponse;
import org.apache.inlong.manager.common.pojo.workflow.TaskResponse;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.common.pojo.workflow.form.process.ApplyGroupProcessForm;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.springframework.boot.configurationprocessor.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Inlong group service implementation.
 */
public class InlongGroupImpl implements InlongGroup {

    public static final String GROUP_FIELD = "groupInfo";
    public static final String MQ_FIELD_OLD = "middlewareType";
    public static final String MQ_FIELD = "mqType";

    private final InnerGroupContext groupContext;
    private InlongGroupInfo groupInfo;
    private final InlongGroupClient groupClient;
    private final WorkflowClient workFlowClient;
    private final InlongStreamClient streamClient;
    private final ClientConfiguration configuration;

    public InlongGroupImpl(InlongGroupInfo groupInfo, ClientConfiguration configuration) {
        this.groupInfo = groupInfo;
        this.groupContext = new InnerGroupContext();
        this.groupContext.setGroupInfo(groupInfo);
        this.configuration = configuration;

        ClientFactory clientFactory = ClientUtils.getClientFactory(configuration);
        this.streamClient = clientFactory.getStreamClient();
        this.groupClient = clientFactory.getGroupClient();
        this.workFlowClient = clientFactory.getWorkflowClient();

        InlongGroupInfo newGroupInfo = groupClient.getGroupIfExists(groupInfo.getInlongGroupId());
        if (newGroupInfo != null) {
            this.groupContext.setGroupInfo(groupInfo);
        } else {
            BaseSortConf sortConf = groupInfo.getSortConf();
            InlongGroupTransfer.createGroupInfo(groupInfo, sortConf);
            String groupId = groupClient.createGroup(groupInfo.genRequest());
            groupInfo.setInlongGroupId(groupId);
        }
    }

    @Override
    public InlongStreamBuilder createStream(InlongStreamInfo streamInfo) {
        return new DefaultInlongStreamBuilder(streamInfo, this.groupContext, configuration);
    }

    @Override
    public InlongGroupContext context() throws Exception {
        return generateSnapshot();
    }

    @Override
    public InlongGroupContext init() throws Exception {
        InlongGroupInfo groupInfo = this.groupContext.getGroupInfo();
        WorkflowResult initWorkflowResult = groupClient.initInlongGroup(groupInfo.genRequest());
        List<TaskResponse> taskViews = initWorkflowResult.getNewTasks();
        Preconditions.checkNotEmpty(taskViews, "init inlong group info failed");
        TaskResponse taskView = taskViews.get(0);
        final int taskId = taskView.getId();
        ProcessResponse processView = initWorkflowResult.getProcessInfo();
        Preconditions.checkTrue(ProcessStatus.PROCESSING == processView.getStatus(),
                String.format("process status %s is not corrected, should be PROCESSING", processView.getStatus()));

        // init must be ApplyGroupProcessForm
        // compile with old cluster
        JSONObject formDataJson = JsonUtils.parseObject(
                JsonUtils.toJsonString(JsonUtils.toJsonString(processView.getFormData())),
                JSONObject.class);
        assert formDataJson != null;
        if (formDataJson.has(GROUP_FIELD)) {
            JSONObject groupInfoJson = formDataJson.getJSONObject(GROUP_FIELD);
            if (groupInfoJson.has(MQ_FIELD_OLD) && !groupInfoJson.has(MQ_FIELD)) {
                groupInfoJson.put(MQ_FIELD, groupInfoJson.get(MQ_FIELD_OLD));
            }
        }
        String formDataNew = formDataJson.toString();
        ApplyGroupProcessForm groupProcessForm = JsonUtils.parseObject(
                formDataNew, ApplyGroupProcessForm.class);
        Preconditions.checkNotNull(groupProcessForm, "ApplyGroupProcessForm cannot be null");
        groupContext.setInitMsg(groupProcessForm);
        assert groupProcessForm != null;
        WorkflowResult startWorkflowResult = workFlowClient.startInlongGroup(taskId, groupProcessForm);
        processView = startWorkflowResult.getProcessInfo();
        Preconditions.checkTrue(ProcessStatus.COMPLETED == processView.getStatus(),
                String.format("inlong group status %s is incorrect, should be COMPLETED", processView.getStatus()));
        return generateSnapshot();
    }

    @Override
    public void update(InlongGroupInfo originGroupInfo, BaseSortConf sortConf) throws Exception {
        if (originGroupInfo == null) {
            originGroupInfo = this.groupInfo;
        }

        final String groupId = originGroupInfo.getInlongGroupId();
        Preconditions.checkTrue(groupId != null && groupId.equals(this.groupInfo.getInlongGroupId()),
                "groupId must be same");

        InlongGroupInfo existGroupInfo = groupClient.getGroupInfo(groupId);
        SimpleGroupStatus status = SimpleGroupStatus.parseStatusByCode(existGroupInfo.getStatus());
        Preconditions.checkTrue(status != SimpleGroupStatus.INITIALIZING,
                "inlong group is in init status, should not be updated");

        InlongGroupInfo groupInfo = InlongGroupTransfer.createGroupInfo(originGroupInfo, sortConf);
        InlongGroupRequest groupRequest = groupInfo.genRequest();
        Pair<String, String> idAndErr = groupClient.updateGroup(groupRequest);
        String errMsg = idAndErr.getValue();
        Preconditions.checkNull(errMsg, errMsg);

        this.groupContext.setGroupInfo(groupInfo);
        this.groupInfo = groupInfo;
    }

    @Override
    public void update(BaseSortConf sortConf) throws Exception {
        Preconditions.checkNotNull(sortConf, "sort conf cannot be null");

        final String groupId = this.groupInfo.getInlongGroupId();
        InlongGroupInfo groupInfo = groupClient.getGroupInfo(groupId);

        SimpleGroupStatus status = SimpleGroupStatus.parseStatusByCode(groupInfo.getStatus());
        Preconditions.checkTrue(status != SimpleGroupStatus.INITIALIZING,
                "inlong group is in init status, should not be updated");

        groupInfo = InlongGroupTransfer.createGroupInfo(this.groupInfo, sortConf);
        InlongGroupRequest groupRequest = groupInfo.genRequest();
        Pair<String, String> idAndErr = groupClient.updateGroup(groupRequest);
        String errMsg = idAndErr.getValue();
        Preconditions.checkNull(errMsg, errMsg);
        this.groupContext.setGroupInfo(groupInfo);
    }

    @Override
    public InlongGroupContext reInitOnUpdate(InlongGroupInfo originGroupInfo, BaseSortConf sortConf) throws Exception {
        this.update(originGroupInfo, sortConf);
        String inlongGroupId = this.groupContext.getGroupInfo().getInlongGroupId();
        InlongGroupInfo newGroupInfo = groupClient.getGroupIfExists(inlongGroupId);
        if (newGroupInfo != null) {
            this.groupContext.setGroupInfo(newGroupInfo);
        } else {
            throw new RuntimeException(String.format("Group not found by inlongGroupId=%s", inlongGroupId));
        }

        return init();
    }

    @Override
    public InlongGroupContext suspend() {
        return suspend(false);
    }

    @Override
    public InlongGroupContext suspend(boolean async) {
        InlongGroupInfo groupInfo = groupContext.getGroupInfo();
        Pair<String, String> idAndErr = groupClient.updateGroup(groupInfo.genRequest());
        final String errMsg = idAndErr.getValue();
        final String groupId = idAndErr.getKey();
        Preconditions.checkNull(errMsg, errMsg);
        groupClient.operateInlongGroup(groupId, SimpleGroupStatus.STOPPED, async);
        return generateSnapshot();
    }

    @Override
    public InlongGroupContext restart() {
        return restart(false);
    }

    @Override
    public InlongGroupContext restart(boolean async) {
        InlongGroupInfo groupInfo = groupContext.getGroupInfo();
        Pair<String, String> idAndErr = groupClient.updateGroup(groupInfo.genRequest());
        final String errMsg = idAndErr.getValue();
        final String groupId = idAndErr.getKey();
        Preconditions.checkNull(errMsg, errMsg);
        groupClient.operateInlongGroup(groupId, SimpleGroupStatus.STARTED, async);
        return generateSnapshot();
    }

    @Override
    public InlongGroupContext delete() throws Exception {
        return delete(false);
    }

    @Override
    public InlongGroupContext delete(boolean async) throws Exception {
        InlongGroupInfo groupInfo = groupClient.getGroupInfo(groupContext.getGroupId());
        boolean isDeleted = groupClient.deleteInlongGroup(groupInfo.getInlongGroupId(), async);
        if (isDeleted) {
            groupInfo.setStatus(GroupStatus.DELETED.getCode());
        }
        return generateSnapshot();
    }

    @Override
    public List<InlongStream> listStreams() {
        String inlongGroupId = this.groupContext.getGroupId();
        return fetchInlongStreams(inlongGroupId);
    }

    @Override
    public InlongGroupContext reset(int rerun, int resetFinalStatus) {
        InlongGroupInfo groupInfo = groupContext.getGroupInfo();
        InlongGroupResetRequest request = new InlongGroupResetRequest(groupInfo.getInlongGroupId(),
                rerun, resetFinalStatus);
        groupClient.resetGroup(request);
        return generateSnapshot();
    }

    private InlongGroupContext generateSnapshot() {
        // fetch current group
        InlongGroupInfo groupInfo = groupClient.getGroupInfo(groupContext.getGroupId());
        groupContext.setGroupInfo(groupInfo);
        String inlongGroupId = groupInfo.getInlongGroupId();
        // fetch stream in group
        List<InlongStream> dataStreams = fetchInlongStreams(inlongGroupId);
        if (CollectionUtils.isNotEmpty(dataStreams)) {
            dataStreams.forEach(groupContext::setStream);
        }

        // create group context
        InlongGroupContext inlongGroupContext = new InlongGroupContext(groupContext);
        // fetch group logs
        List<EventLogView> logViews = workFlowClient.getInlongGroupError(inlongGroupId);
        if (CollectionUtils.isNotEmpty(logViews)) {
            Map<String, List<String>> errMsgMap = Maps.newHashMap();
            Map<String, List<String>> groupLogMap = Maps.newHashMap();
            logViews.stream()
                    .filter(x -> StringUtils.isNotEmpty(x.getElementName()))
                    .forEach(eventLogView -> {
                        String taskName = eventLogView.getElementName();
                        if (StringUtils.isNotEmpty(eventLogView.getException())) {
                            errMsgMap.computeIfAbsent(taskName, Lists::newArrayList).add(eventLogView.getException());
                        }
                        if (StringUtils.isNotEmpty(eventLogView.getRemark())) {
                            groupLogMap.computeIfAbsent(taskName, Lists::newArrayList).add(eventLogView.getRemark());
                        }
                    });
            inlongGroupContext.setGroupErrLogs(errMsgMap);
            inlongGroupContext.setGroupLogs(groupLogMap);
        }

        // fetch stream logs
        Map<String, InlongStream> streams = inlongGroupContext.getInlongStreamMap();
        streams.keySet().forEach(streamId -> {
            List<InlongStreamConfigLogListResponse> logList = streamClient.getStreamLogs(inlongGroupId, streamId);
            if (CollectionUtils.isNotEmpty(logList)) {
                Map<String, List<String>> streamLogs = Maps.newHashMap();
                logList.stream().filter(x -> StringUtils.isNotEmpty(x.getComponentName()))
                        .forEach(streamLog -> {
                            String componentName = streamLog.getComponentName();
                            String log = JsonUtils.toJsonString(streamLog);
                            streamLogs.computeIfAbsent(componentName, Lists::newArrayList).add(log);
                        });
                inlongGroupContext.getStreamErrLogs().put(streamId, streamLogs);
            }
        });
        return inlongGroupContext;
    }

    private List<InlongStream> fetchInlongStreams(String groupId) {
        List<InlongStreamInfo> streamInfos = streamClient.listStreamInfo(groupId);
        if (CollectionUtils.isEmpty(streamInfos)) {
            return null;
        }
        return streamInfos.stream()
                .map(streamInfo -> new InlongStreamImpl(streamInfo, configuration))
                .collect(Collectors.toList());
    }
}
