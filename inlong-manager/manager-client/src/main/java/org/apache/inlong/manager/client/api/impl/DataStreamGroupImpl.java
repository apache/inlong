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
import org.apache.inlong.manager.client.api.DataStream;
import org.apache.inlong.manager.client.api.DataStreamBuilder;
import org.apache.inlong.manager.client.api.DataStreamConf;
import org.apache.inlong.manager.client.api.DataStreamGroup;
import org.apache.inlong.manager.client.api.DataStreamGroupConf;
import org.apache.inlong.manager.client.api.DataStreamGroupInfo;
import org.apache.inlong.manager.client.api.DataStreamGroupInfo.GroupState;
import org.apache.inlong.manager.client.api.inner.InnerGroupContext;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.client.api.util.AssertUtil;
import org.apache.inlong.manager.client.api.util.DataStreamGroupTransfer;
import org.apache.inlong.manager.client.api.util.InlongParser;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.enums.ProcessStatus;
import org.apache.inlong.manager.common.pojo.business.BusinessApproveInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamApproveInfo;
import org.apache.inlong.manager.common.pojo.datastream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.workflow.ProcessResponse;
import org.apache.inlong.manager.common.pojo.workflow.TaskResponse;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataStreamGroupImpl implements DataStreamGroup {

    private DataStreamGroupConf groupConf;

    private InnerInlongManagerClient managerClient;

    private InnerGroupContext groupContext;

    public DataStreamGroupImpl(DataStreamGroupConf groupConf, InlongClientImpl inlongClient) {
        this.groupConf = groupConf;
        this.groupContext = new InnerGroupContext();
        BusinessInfo businessInfo = DataStreamGroupTransfer.createBusinessInfo(groupConf);
        this.groupContext.setBusinessInfo(businessInfo);
        if (this.managerClient == null) {
            this.managerClient = new InnerInlongManagerClient(inlongClient);
        }
        Pair<Boolean, BusinessInfo> existMsg = managerClient.isBusinessExists(businessInfo);
        if (existMsg.getKey()) {
            //Update current snapshot
            this.groupContext.setBusinessInfo(existMsg.getValue());
        } else {
            String groupId = managerClient.createBusinessInfo(businessInfo);
            businessInfo.setInlongGroupId(groupId);
        }
    }

    @Override
    public DataStreamBuilder createDataStream(DataStreamConf dataStreamConf) throws Exception {
        return new DefaultDataStreamBuilder(dataStreamConf, this.groupContext, this.managerClient);
    }

    @Override
    public DataStreamGroupInfo init() throws Exception {
        WorkflowResult initWorkflowResult = managerClient.initBusinessGroup(this.groupContext.getBusinessInfo());
        List<TaskResponse> tasks = initWorkflowResult.getNewTasks();
        AssertUtil.notEmpty(tasks, "Init business info failed");
        TaskResponse task = tasks.get(0);
        final int taskId = task.getId();

        ProcessResponse processResponse = initWorkflowResult.getProcessInfo();
        AssertUtil.isTrue(ProcessStatus.PROCESSING == processResponse.getStatus(),
                String.format("Process status: %s is not corrected, should be PROCESSING",
                        processResponse.getStatus()));
        String formData = processResponse.getFormData().toString();
        Pair<BusinessApproveInfo, List<DataStreamApproveInfo>> initMsg = InlongParser.parseBusinessForm(formData);
        groupContext.setInitMsg(initMsg);
        WorkflowResult startWorkflowResult = managerClient.startBusinessGroup(taskId, initMsg);
        processResponse = startWorkflowResult.getProcessInfo();
        AssertUtil.isTrue(ProcessStatus.COMPLETED == processResponse.getStatus(),
                String.format("Business info state : %s is not corrected , should be COMPLETED",
                        processResponse.getStatus()));
        return generateSnapshot(null);
    }

    @Override
    public DataStreamGroupInfo suspend() {
        Pair<String, String> idAndErr = managerClient.updateBusinessInfo(groupContext.getBusinessInfo());
        final String errMsg = idAndErr.getValue();
        final String groupId = idAndErr.getKey();
        AssertUtil.isNull(errMsg, errMsg);
        managerClient.operateBusinessGroup(groupId, GroupState.SUSPEND);
        return generateSnapshot(null);
    }

    @Override
    public DataStreamGroupInfo restart() {
        Pair<String, String> idAndErr = managerClient.updateBusinessInfo(groupContext.getBusinessInfo());
        final String errMsg = idAndErr.getValue();
        final String groupId = idAndErr.getKey();
        AssertUtil.isNull(errMsg, errMsg);
        managerClient.operateBusinessGroup(groupId, GroupState.RESTART);
        return generateSnapshot(null);
    }

    @Override
    public DataStreamGroupInfo delete() {
        BusinessInfo currentBusinessInfo = managerClient.getBusinessInfo(
                groupContext.getBusinessInfo().getInlongGroupId());
        boolean isDeleted = managerClient.deleteBusinessGroup(currentBusinessInfo.getInlongGroupId());
        if (isDeleted) {
            currentBusinessInfo.setStatus(EntityStatus.DELETED.getCode());
        }
        return generateSnapshot(currentBusinessInfo);
    }

    @Override
    public List<DataStream> listStreams() {
        String inlongGroupId = this.groupContext.getGroupId();
        return fetchDataStreams(inlongGroupId);
    }

    private DataStreamGroupInfo generateSnapshot(BusinessInfo currentBizInfo) {
        if (currentBizInfo == null) {
            currentBizInfo = managerClient.getBusinessInfo(
                    groupContext.getBusinessInfo().getInlongGroupId());
        }
        String inlongGroupId = currentBizInfo.getInlongGroupId();
        List<DataStream> dataStreams = fetchDataStreams(inlongGroupId);
        dataStreams.forEach(dataStream -> groupContext.setStream(dataStream));
        return new DataStreamGroupInfo(groupContext, groupConf);
    }

    private List<DataStream> fetchDataStreams(String groupId) {
        List<FullStreamResponse> streamResponses = managerClient.listStreamInfo(groupId);
        List<DataStream> streamList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(streamResponses)) {
            streamList = streamResponses.stream().map(DataStreamImpl::new).collect(Collectors.toList());
        }
        return streamList;
    }

}
