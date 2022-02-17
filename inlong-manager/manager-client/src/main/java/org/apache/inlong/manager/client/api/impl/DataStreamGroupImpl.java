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

import java.util.List;
import javafx.util.Pair;
import org.apache.inlong.manager.client.api.DataStream;
import org.apache.inlong.manager.client.api.DataStreamBuilder;
import org.apache.inlong.manager.client.api.DataStreamConf;
import org.apache.inlong.manager.client.api.DataStreamGroup;
import org.apache.inlong.manager.client.api.DataStreamGroupConf;
import org.apache.inlong.manager.client.api.DataStreamGroupInfo;
import org.apache.inlong.manager.client.api.inner.InnerGroupContext;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.client.api.util.DataStreamGroupTransfer;
import org.apache.inlong.manager.client.api.util.InlongParser;
import org.apache.inlong.manager.common.model.ProcessState;
import org.apache.inlong.manager.common.model.view.ProcessView;
import org.apache.inlong.manager.common.model.view.TaskView;
import org.apache.inlong.manager.common.pojo.business.BusinessApproveInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamApproveInfo;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.shiro.util.Assert;

public class DataStreamGroupImpl implements DataStreamGroup {

    private DataStreamGroupConf conf;

    private InlongClientImpl inlongClient;

    private InnerInlongManagerClient managerClient;

    private InnerGroupContext groupContext;

    public DataStreamGroupImpl(DataStreamGroupConf conf, InlongClientImpl inlongClient) {
        this.conf = conf;
        this.inlongClient = inlongClient;
        this.groupContext = new InnerGroupContext();
        BusinessInfo businessInfo = DataStreamGroupTransfer.createBusinessInfo(conf);
        this.groupContext.setBusinessInfo(businessInfo);
        if (this.managerClient == null) {
            this.managerClient = new InnerInlongManagerClient(inlongClient);
        }
        if (!managerClient.isBusinessExists(businessInfo)) {
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
        WorkflowResult initWorkflowResult = managerClient.initBusinessInfo(this.groupContext.getBusinessInfo());
        List<TaskView> taskViews = initWorkflowResult.getNewTasks();
        Assert.notEmpty(taskViews, "Init business info failed");
        TaskView taskView = taskViews.get(0);
        final int taskId = taskView.getId();
        ProcessView processView = initWorkflowResult.getProcessInfo();
        Assert.isTrue(ProcessState.PROCESSING == processView.getState(),
                String.format("Business info state : %s is not corrected , should be PROCESSING",
                        processView.getState()));
        String formData = processView.getFormData().toString();
        Pair<BusinessApproveInfo, List<DataStreamApproveInfo>> initMsg = InlongParser.parseBusinessForm(formData);
        groupContext.setInitMsg(initMsg);
        WorkflowResult startWorkflowResult = managerClient.startBusinessInfo(taskId,initMsg);
        processView = startWorkflowResult.getProcessInfo();
        Assert.isTrue(ProcessState.COMPLETED == processView.getState(),
                String.format("Business info state : %s is not corrected , should be COMPLETED",
                        processView.getState()));

        //todo get business status
        return null;
    }

    @Override
    public DataStreamGroupInfo suspend() throws Exception {
        //todo update businessInfo first
        return null;
    }

    @Override
    public DataStreamGroupInfo restart() throws Exception {
        //todo update businessInfo first
        return null;
    }

    @Override
    public DataStreamGroupInfo delete() throws Exception {
        //todo update businessInfo first
        return null;
    }

    @Override
    public List<DataStream> listStreams(String streamGroupId) throws Exception {
        return null;
    }
}
