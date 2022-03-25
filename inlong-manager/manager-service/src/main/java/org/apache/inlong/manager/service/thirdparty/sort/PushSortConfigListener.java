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

import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.service.CommonOperateService;
import org.apache.inlong.manager.service.core.InlongGroupService;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.inlong.sort.ZkTools;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.InLongMsgCsvDeserializationInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;
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
    private CommonOperateService commonOperateService;
    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private StreamSinkService streamSinkService;

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

            DataFlowInfo dataFlowInfo = commonOperateService.createDataFlow(groupInfo, sinkResponse);
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

    /**
     * Get source info for DataFlowInfo
     */
    @Deprecated
    private SourceInfo getSourceInfo(InlongGroupInfo groupInfo, String streamId,
            List<StreamSinkFieldEntity> fieldList) {
        DeserializationInfo deserializationInfo = null;
        String groupId = groupInfo.getInlongGroupId();
        InlongStreamInfo streamInfo = streamService.get(groupId, streamId);

        boolean isDbType = Constant.DATA_SOURCE_DB.equals(streamInfo.getDataType());
        if (!isDbType) {
            // FILE and auto push source, the data format is TEXT or KEY-VALUE, temporarily use InLongMsgCsv
            String dataType = streamInfo.getDataType();
            if (Constant.DATA_TYPE_TEXT.equalsIgnoreCase(dataType)
                    || Constant.DATA_TYPE_KEY_VALUE.equalsIgnoreCase(dataType)) {
                // Use the field separator from the inlong stream
                char separator = (char) Integer.parseInt(streamInfo.getDataSeparator());
                // TODO support escape
                /*Character escape = null;
                if (info.getDataEscapeChar() != null) {
                    escape = info.getDataEscapeChar().charAt(0);
                }*/
                // Whether to delete the first separator, the default is false for the time being
                deserializationInfo = new InLongMsgCsvDeserializationInfo(streamId, separator);
            }
        }

        // The number and order of the source fields must be the same as the target fields
        return null;
    }

    @Override
    public boolean async() {
        return false;
    }

}
