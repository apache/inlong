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

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.service.CommonOperateService;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.inlong.sort.ZkTools;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.InLongMsgCsvDeserializationInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class PushSortConfigListener implements SortOperateListener {

    private static final String DATA_FLOW_GROUP_ID_KEY = "inlong.group.id";

    @Autowired
    private CommonOperateService commonOperateService;
    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private StreamSinkService streamSinkService;
    @Autowired
    private StreamSinkFieldEntityMapper streamSinkFieldMapper;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        if (log.isDebugEnabled()) {
            log.debug("begin push hive config to sort, context={}", context);
        }

        GroupResourceProcessForm form = (GroupResourceProcessForm) context.getProcessForm();
        InlongGroupRequest groupInfo = form.getGroupInfo();
        String groupId = groupInfo.getInlongGroupId();

        InlongGroupEntity groupEntity = groupMapper.selectByGroupId(groupId);
        if (groupEntity == null || EntityStatus.IS_DELETED.getCode().equals(groupEntity.getIsDeleted())) {
            log.warn("skip to push sort hive config for groupId={}, as biz not exists or has been deleted", groupId);
            return ListenerResult.success();
        }

        // if streamId not null, just push the config belongs to the groupId and the streamId
        String streamId = form.getInlongStreamId();
        List<SinkResponse> sinkResponses = streamSinkService.listSink(groupId, streamId);
        for (SinkResponse sinkResponse : sinkResponses) {
            Integer sinkId = sinkResponse.getId();

            if (log.isDebugEnabled()) {
                log.debug("sink info: {}", sinkResponse);
            }

            DataFlowInfo dataFlowInfo = getDataFlowInfo(groupInfo, sinkResponse);
            // add extra properties for flow info
            dataFlowInfo.getProperties().put(DATA_FLOW_GROUP_ID_KEY, groupId);
            if (log.isDebugEnabled()) {
                log.debug("try to push hive config to sort: {}", JsonUtils.toJson(dataFlowInfo));
            }
            try {
                String zkUrl = clusterBean.getZkUrl();
                String zkRoot = clusterBean.getZkRoot();
                // push data flow info to zk
                String sortClusterName = clusterBean.getAppName();
                ZkTools.updateDataFlowInfo(dataFlowInfo, sortClusterName, sinkId, zkUrl, zkRoot);
                // add sink id to zk
                ZkTools.addDataFlowToCluster(sortClusterName, sinkId, zkUrl, zkRoot);
            } catch (Exception e) {
                log.error("add or update inlong stream information to zk failed, sinkId={} ", sinkId, e);
                throw new WorkflowListenerException("push hive config to sort failed, reason: " + e.getMessage());
            }
        }

        return ListenerResult.success();
    }

    private DataFlowInfo getDataFlowInfo(InlongGroupRequest groupInfo, SinkResponse sinkResponse) {
        String groupId = sinkResponse.getInlongGroupId();
        String streamId = sinkResponse.getInlongStreamId();
        List<StreamSinkFieldEntity> fieldList = streamSinkFieldMapper.selectFields(groupId, streamId);

        if (fieldList == null || fieldList.size() == 0) {
            throw new WorkflowListenerException("no hive fields for groupId=" + groupId + ", streamId=" + streamId);
        }

        SourceInfo sourceInfo = getSourceInfo(groupInfo, sinkResponse, fieldList);
        SinkInfo sinkInfo = SinkInfoUtils.createSinkInfo(sinkResponse);

        // push information
        return new DataFlowInfo(sinkResponse.getId(), sourceInfo, sinkInfo);
    }

    /**
     * Get source info
     */
    private SourceInfo getSourceInfo(InlongGroupRequest groupInfo,
                                     SinkResponse hiveResponse, List<StreamSinkFieldEntity> fieldList) {
        DeserializationInfo deserializationInfo = null;
        String groupId = groupInfo.getInlongGroupId();
        String streamId = hiveResponse.getInlongStreamId();
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
                deserializationInfo = new InLongMsgCsvDeserializationInfo(hiveResponse.getInlongStreamId(), separator);
            }
        }

        // The number and order of the source fields must be the same as the target fields
        SourceInfo sourceInfo = null;
        // Get the source field, if there is no partition field in source, add the partition field to the end
        List<FieldInfo> sourceFields = getSourceFields(fieldList);

        String middleWare = groupInfo.getMiddlewareType();
        if (Constant.MIDDLEWARE_TUBE.equalsIgnoreCase(middleWare)) {
            String masterAddress = commonOperateService.getSpecifiedParam(Constant.TUBE_MASTER_URL);
            Preconditions.checkNotNull(masterAddress, "tube cluster address cannot be empty");
            String topic = groupInfo.getMqResourceObj();
            // The consumer group name is: taskName_topicName_consumer_group
            String consumerGroup = clusterBean.getAppName() + "_" + topic + "_consumer_group";
            sourceInfo = new TubeSourceInfo(topic, masterAddress, consumerGroup,
                    deserializationInfo, sourceFields.toArray(new FieldInfo[0]));
        } else if (Constant.MIDDLEWARE_PULSAR.equalsIgnoreCase(middleWare)) {
            String pulsarAdminUrl = commonOperateService.getSpecifiedParam(Constant.PULSAR_ADMINURL);
            String pulsarServiceUrl = commonOperateService.getSpecifiedParam(Constant.PULSAR_SERVICEURL);
            sourceInfo = SourceInfoUtils.createPulsarSourceInfo(groupInfo, streamInfo.getMqResourceObj(),
                    deserializationInfo, sourceFields, clusterBean.getAppName(), clusterBean.getDefaultTenant(),
                    pulsarAdminUrl, pulsarServiceUrl);
        }
        return sourceInfo;
    }

    /**
     * Get source field list
     * TODO  support BuiltInField
     */
    private List<FieldInfo> getSourceFields(List<StreamSinkFieldEntity> fieldList) {
        List<FieldInfo> fieldInfoList = new ArrayList<>();
        for (StreamSinkFieldEntity field : fieldList) {
            FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(field.getSourceFieldType().toLowerCase());
            String fieldName = field.getSourceFieldName();

            FieldInfo fieldInfo = new FieldInfo(fieldName, formatInfo);
            fieldInfoList.add(fieldInfo);
        }

        return fieldInfoList;
    }

    @Override
    public boolean async() {
        return false;
    }

}
