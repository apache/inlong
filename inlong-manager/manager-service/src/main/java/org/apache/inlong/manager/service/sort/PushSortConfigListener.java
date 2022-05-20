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

package org.apache.inlong.manager.service.sort;

import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.sort.util.DataFlowUtils;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.inlong.sort.ZkTools;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Push sort config when enable the ZooKeeper
 */
@Component
public class PushSortConfigListener implements SortOperateListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushSortConfigListener.class);

    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private StreamSinkService streamSinkService;
    @Autowired
    private DataFlowUtils dataFlowUtils;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to push sort config by context={}", context);
        }

        GroupResourceProcessForm form = (GroupResourceProcessForm) context.getProcessForm();
        String groupId = form.getGroupInfo().getInlongGroupId();
        InlongGroupInfo groupInfo = groupService.get(groupId);

        // if streamId not null, just push the config belongs to the groupId and the streamId
        String streamId = form.getInlongStreamId();
        List<SinkResponse> sinkResponseList = streamSinkService.listSink(groupId, streamId);
        if (CollectionUtils.isEmpty(sinkResponseList)) {
            LOGGER.warn("Sink not found by groupId={}", groupId);
            return ListenerResult.success();
        }

        for (SinkResponse sinkResponse : sinkResponseList) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sink info: {}", sinkResponse);
            }

            DataFlowInfo dataFlowInfo = dataFlowUtils.createDataFlow(groupInfo, sinkResponse);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("try to push config to sort: {}", JsonUtils.toJson(dataFlowInfo));
            }

            Integer sinkId = sinkResponse.getId();
            try {
                String zkUrl = clusterBean.getZkUrl();
                String zkRoot = clusterBean.getZkRoot();
                // push data flow info to zk
                String sortClusterName = clusterBean.getAppName();
                ZkTools.updateDataFlowInfo(dataFlowInfo, sortClusterName, sinkId, zkUrl, zkRoot);
                // add sink id to zk
                ZkTools.addDataFlowToCluster(sortClusterName, sinkId, zkUrl, zkRoot);
            } catch (Exception e) {
                LOGGER.error("push sort config to zookeeper failed, sinkId={} ", sinkId, e);
                throw new WorkflowListenerException("push sort config to zookeeper failed: " + e.getMessage());
            }
        }

        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return false;
    }

}
