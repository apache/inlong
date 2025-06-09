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

package org.apache.inlong.manager.service.resource.sort;

import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.common.enums.MessageWrapType;
import org.apache.inlong.common.pojo.sort.dataflow.DataFlowConfig;
import org.apache.inlong.common.pojo.sort.dataflow.SourceConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.DataTypeConfig;
import org.apache.inlong.common.pojo.sort.dataflow.deserialization.DeserializationConfig;
import org.apache.inlong.common.pojo.sort.dataflow.field.FieldConfig;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.sink.SinkConfig;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.SortConfigEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.SortConfigEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterDTO;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sort.util.FieldInfoUtils;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.datatype.DataTypeOperator;
import org.apache.inlong.manager.service.datatype.DataTypeOperatorFactory;
import org.apache.inlong.manager.service.message.DeserializeOperator;
import org.apache.inlong.manager.service.message.DeserializeOperatorFactory;
import org.apache.inlong.manager.service.sink.SinkOperatorFactory;
import org.apache.inlong.manager.service.sink.StreamSinkOperator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.inlong.manager.service.resource.queue.pulsar.PulsarQueueResourceOperator.PULSAR_SUBSCRIPTION;
import static org.apache.inlong.manager.service.resource.queue.tubemq.TubeMQQueueResourceOperator.TUBE_CONSUMER_GROUP;

@Service
public class DefaultSortConfigOperator implements SortConfigOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSortConfigOperator.class);

    @Autowired
    public DeserializeOperatorFactory deserializeOperatorFactory;
    @Autowired
    public DataTypeOperatorFactory dataTypeOperatorFactory;
    @Autowired
    private StreamSinkFieldEntityMapper sinkFieldMapper;
    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private SortConfigEntityMapper sortConfigEntityMapper;
    @Autowired
    private InlongGroupEntityMapper groupEntityMapper;
    @Autowired
    private SinkOperatorFactory operatorFactory;

    @Override
    public Boolean accept(List<String> sinkTypeList) {
        for (String sinkType : sinkTypeList) {
            if (SinkType.SORT_STANDALONE_SINK.contains(sinkType)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void buildConfig(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, boolean isStream) throws Exception {
        if (groupInfo == null || streamInfo == null) {
            LOGGER.warn("group info is null or stream infos is empty, no need to build sort config");
            return;
        }

        if (!groupInfo.getInlongGroupMode().equals(InlongConstants.STANDARD_MODE)) {
            return;
        }

        if (isStream) {
            LOGGER.info("no need to build all sort config since the workflow is not stream level, groupId={}",
                    groupInfo.getInlongGroupId());
            return;
        }

        List<StreamSink> sinkList = new ArrayList<>();
        for (StreamSink sink : streamInfo.getSinkList()) {
            if (SinkType.SORT_STANDALONE_SINK.contains(sink.getSinkType())) {
                sinkList.add(sink);
            }
        }
        if (CollectionUtils.isEmpty(sinkList)) {
            return;
        }
        for (StreamSink sink : sinkList) {
            saveDataFlow(groupInfo, streamInfo, sink);
        }

    }

    private void saveDataFlow(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, StreamSink sink) {
        try {
            DataFlowConfig dataFlowConfig = getDataFlowConfig(groupInfo, streamInfo, sink);

            SortConfigEntity sortConfigEntity = sortConfigEntityMapper.selectBySinkId(sink.getId());
            String clusterTags = groupInfo.getInlongClusterTag();
            ObjectMapper objectMapper = new ObjectMapper();
            if (sortConfigEntity == null) {
                dataFlowConfig.setVersion(0);
                sortConfigEntity = CommonBeanUtils.copyProperties(sink, SortConfigEntity::new);
                sortConfigEntity.setId(null);
                if (StringUtils.isBlank(sortConfigEntity.getSortTaskName())) {
                    sortConfigEntity.setSortTaskName(InlongConstants.DEFAULT_TASK);
                }
                sortConfigEntity.setSinkId(sink.getId());
                sortConfigEntity.setConfigParams(objectMapper.writeValueAsString(dataFlowConfig));
                sortConfigEntity.setInlongClusterTag(clusterTags);
                sortConfigEntityMapper.insert(sortConfigEntity);
            } else {
                dataFlowConfig.setVersion(sortConfigEntity.getVersion() + 1);
                sortConfigEntity.setInlongClusterName(sink.getInlongClusterName());
                sortConfigEntity.setDataNodeName(sink.getDataNodeName());
                sortConfigEntity.setSortTaskName(sink.getSortTaskName());
                sortConfigEntity.setConfigParams(objectMapper.writeValueAsString(dataFlowConfig));
                sortConfigEntity.setInlongClusterTag(clusterTags);
                if (StringUtils.isBlank(sortConfigEntity.getSortTaskName())) {
                    sortConfigEntity.setSortTaskName(InlongConstants.DEFAULT_TASK);
                }
                sortConfigEntityMapper.updateByIdSelective(sortConfigEntity);
            }
        } catch (Exception e) {
            LOGGER.error("failed to parse id params of groupId={}, streamId={} name={}, type={}",
                    sink.getInlongGroupId(), sink.getInlongStreamId(),
                    sink.getSinkName(), sink.getSinkType(), e);
        }
    }

    private DataFlowConfig getDataFlowConfig(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, StreamSink sink) {
        HashMap<String, Object> properties = new HashMap<>();
        return DataFlowConfig.builder()
                .dataflowId(String.valueOf(sink.getId()))
                .sourceConfig(getSourceConfig(groupInfo, streamInfo, sink))
                .auditTag(String.valueOf(sink.getId()))
                .transformSql(sink.getTransformSql())
                .sinkConfig(getSinkConfig(groupInfo, streamInfo, sink))
                .inlongGroupId(groupInfo.getInlongGroupId())
                .inlongStreamId(streamInfo.getInlongStreamId())
                .properties(properties)
                .build();
    }

    private SinkConfig getSinkConfig(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, StreamSink sink) {
        StreamSinkOperator sinkOperator = operatorFactory.getInstance(sink.getSinkType());
        return sinkOperator.getSinkConfig(groupInfo, streamInfo, sink);
    }

    private SourceConfig getSourceConfig(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, StreamSink sink) {
        String topic = "";
        String fullTopic;
        String subs = "";
        switch (groupInfo.getMqType()) {
            case MQType.TUBEMQ:
                fullTopic = groupInfo.getMqResource();
                List<InlongClusterEntity> tubeClusters =
                        clusterMapper.selectByKey(groupInfo.getInlongClusterTag(), null, MQType.TUBEMQ);
                if (CollectionUtils.isEmpty(tubeClusters)) {
                    throw new WorkflowListenerException(
                            "tube cluster not found for groupId=" + groupInfo.getInlongGroupId());
                }
                InlongClusterEntity tubeCluster = tubeClusters.get(0);
                Preconditions.expectNotNull(tubeCluster,
                        "tube cluster not found for groupId=" + groupInfo.getInlongGroupId());
                String masterAddress = tubeCluster.getUrl();
                Preconditions.expectNotNull(masterAddress,
                        "tube cluster [" + tubeCluster.getId() + "] not contains masterAddress");
                subs = String.format(TUBE_CONSUMER_GROUP, groupInfo.getInlongClusterTag(), groupInfo.getMqResource(),
                        sink.getId());
                break;
            case MQType.PULSAR:
                List<InlongClusterEntity> pulsarClusters =
                        clusterMapper.selectByKey(groupInfo.getInlongClusterTag(), null, MQType.PULSAR);
                if (CollectionUtils.isEmpty(pulsarClusters)) {
                    throw new WorkflowListenerException(
                            "pulsar cluster not found for groupId=" + groupInfo.getInlongGroupId());
                }
                InlongClusterEntity pulsarCluster = pulsarClusters.get(0);
                // Multiple adminUrls should be configured for pulsar,
                // otherwise all requests will be sent to the same broker
                PulsarClusterDTO pulsarClusterDTO = PulsarClusterDTO.getFromJson(pulsarCluster.getExtParams());
                if (!(groupInfo instanceof InlongPulsarInfo)) {
                    throw new BusinessException(
                            "the mqType must be PULSAR for inlongGroupId=" + groupInfo.getInlongGroupId());
                }
                InlongPulsarInfo pulsarInfo = (InlongPulsarInfo) groupInfo;
                String tenant = pulsarInfo.getPulsarTenant();
                if (StringUtils.isBlank(tenant) && StringUtils.isNotBlank(pulsarClusterDTO.getPulsarTenant())) {
                    tenant = pulsarClusterDTO.getPulsarTenant();
                }
                if (StringUtils.isBlank(tenant)) {
                    tenant = InlongConstants.DEFAULT_PULSAR_TENANT;
                }

                String namespace = groupInfo.getMqResource();
                topic = streamInfo.getMqResource();
                // Full path of topic in pulsar
                fullTopic = "persistent://" + tenant + "/" + namespace + "/" + topic;
                subs = String.format(PULSAR_SUBSCRIPTION, groupInfo.getInlongClusterTag(), topic,
                        sink.getId());
                break;
            default:
                throw new BusinessException(
                        String.format(ErrorCodeEnum.MQ_TYPE_NOT_SUPPORTED.getMessage(), groupInfo.getMqType()));
        }
        DeserializeOperator deserializeOperator =
                deserializeOperatorFactory.getInstance(MessageWrapType.forType(streamInfo.getWrapType()));
        DeserializationConfig deserializationConfig = deserializeOperator.getDeserializationConfig(streamInfo);
        DataTypeOperator dataTypeOperator =
                dataTypeOperatorFactory.getInstance(DataTypeEnum.forType(streamInfo.getDataType()));
        DataTypeConfig dataTypeConfig = dataTypeOperator.getDataTypeConfig(streamInfo);
        SourceConfig sourceConfig = new SourceConfig();
        List<FieldConfig> fields = streamInfo.getFieldList().stream().map(
                v -> {
                    FieldConfig fieldConfig = new FieldConfig();
                    FormatInfo formatInfo = FieldInfoUtils.convertFieldFormat(
                            v.getFieldType().toLowerCase());
                    fieldConfig.setName(v.getFieldName());
                    fieldConfig.setFormatInfo(formatInfo);
                    return fieldConfig;
                }).collect(Collectors.toList());
        sourceConfig.setFieldConfigs(fields);
        sourceConfig.setDeserializationConfig(deserializationConfig);
        sourceConfig.setDataTypeConfig(dataTypeConfig);
        sourceConfig.setEncodingType(streamInfo.getDataEncoding());
        sourceConfig.setTopic(fullTopic);
        sourceConfig.setSubscription(subs);
        return sourceConfig;
    }

}
