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

import org.apache.inlong.common.constant.ClusterSwitch;
import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.common.pojo.sdk.Topic;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupExtEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamExtEntity;
import org.apache.inlong.manager.dao.entity.SortConfigEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupExtEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamExtEntityMapper;
import org.apache.inlong.manager.dao.mapper.SortConfigEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sort.standalone.SortSourceClusterInfo;
import org.apache.inlong.manager.pojo.sort.standalone.SortSourceGroupInfo;
import org.apache.inlong.manager.pojo.sort.standalone.SortSourceStreamSinkInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.node.DataNodeOperator;
import org.apache.inlong.manager.service.node.DataNodeOperatorFactory;
import org.apache.inlong.manager.service.sink.SinkOperatorFactory;
import org.apache.inlong.manager.service.sink.StreamSinkOperator;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class SortStandaloneConfigOperator implements SortConfigOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortStandaloneConfigOperator.class);
    private static final String KEY_TENANT = "tenant";
    private static final String KEY_TENANT_V2 = "pulsarTenant";
    private static final String STANDALONE_CLUSTER_PREFIX = "SORT_";

    @Autowired
    private StreamSinkFieldEntityMapper sinkFieldMapper;
    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private InlongStreamEntityMapper streamEntityMapper;
    @Autowired
    private DataNodeEntityMapper dataNodeMapper;
    @Autowired
    private StreamSinkEntityMapper sinkEntityMapper;
    @Autowired
    private SinkOperatorFactory sinkOperatorFactory;
    @Autowired
    private DataNodeOperatorFactory dataNodeOperatorFactory;
    @Autowired
    private com.fasterxml.jackson.databind.ObjectMapper objectMapper;
    @Autowired
    private SortConfigEntityMapper sortConfigEntityMapper;
    @Autowired
    private InlongGroupEntityMapper groupEntityMapper;
    @Autowired
    private InlongGroupExtEntityMapper groupExtEntityMapper;
    @Autowired
    private InlongStreamExtEntityMapper streamExtEntityMapper;

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
        InlongGroupEntity groupEntity = groupEntityMapper.selectByGroupId(groupInfo.getInlongGroupId());
        Preconditions.expectTrue(MQType.PULSAR.equals(groupEntity.getMqType()), "Standalone only support pulsar");
        SortSourceGroupInfo sortGroupInfo = CommonBeanUtils.copyProperties(groupEntity, SortSourceGroupInfo::new);
        sortGroupInfo.setGroupId(groupInfo.getInlongGroupId());
        sortGroupInfo.setClusterTag(groupInfo.getInlongClusterTag());
        for (StreamSink sink : streamInfo.getSinkList()) {
            StreamSinkEntity sinkEntity = sinkEntityMapper.selectByPrimaryKey(sink.getId());
            SortSourceStreamSinkInfo sortSink =
                    CommonBeanUtils.copyProperties(sinkEntity, SortSourceStreamSinkInfo::new);
            sortSink.setSortClusterName(sink.getInlongClusterName());
            InlongClusterEntity standAloneCluster = clusterMapper.selectByNameAndType(sinkEntity.getInlongClusterName(),
                    STANDALONE_CLUSTER_PREFIX + sinkEntity.getSinkType());
            if (SinkType.SORT_STANDALONE_SINK.contains(sink.getSinkType())) {
                InlongStreamEntity streamEntity = streamEntityMapper.selectByIdentifier(sink.getInlongGroupId(),
                        sink.getInlongStreamId());
                List<Topic> topicList = new ArrayList<>();
                Topic topic = saveTopic(sortGroupInfo, streamEntity, sortSink);
                topicList.add(topic);
                InlongGroupExtEntity groupBackUpTag =
                        groupExtEntityMapper.selectByUniqueKey(groupEntity.getInlongGroupId(),
                                ClusterSwitch.BACKUP_CLUSTER_TAG);
                InlongGroupExtEntity backUpMqResource =
                        groupExtEntityMapper.selectByUniqueKey(groupEntity.getInlongGroupId(),
                                ClusterSwitch.BACKUP_MQ_RESOURCE);
                InlongStreamExtEntity streamBackUpMqResource = streamExtEntityMapper.selectByKey(
                        sink.getInlongGroupId(), sink.getInlongStreamId(), ClusterSwitch.BACKUP_MQ_RESOURCE);
                Set<String> clusterTags = Sets.newHashSet(
                        standAloneCluster.getClusterTags().split(InlongConstants.COMMA));
                if (groupBackUpTag != null && (CollectionUtils.isEmpty(clusterTags) || clusterTags.contains(
                        groupBackUpTag.getKeyValue()))) {
                    SortSourceGroupInfo backUpSortGroupInfo =
                            CommonBeanUtils.copyProperties(sortGroupInfo, SortSourceGroupInfo::new);
                    backUpSortGroupInfo.setClusterTag(groupBackUpTag.getKeyValue());
                    InlongStreamEntity backUpStreamEntity =
                            CommonBeanUtils.copyProperties(streamEntity, InlongStreamEntity::new);
                    if (backUpMqResource != null) {
                        backUpSortGroupInfo.setMqResource(backUpMqResource.getKeyValue());
                    }
                    if (streamBackUpMqResource != null) {
                        backUpStreamEntity.setMqResource(streamBackUpMqResource.getKeyValue());
                    }
                    Topic backUpTopic = saveTopic(backUpSortGroupInfo, backUpStreamEntity, sortSink);
                    topicList.add(backUpTopic);
                }
                saveSortCluster(groupInfo, sinkEntity, topicList, groupBackUpTag.getKeyValue());
            }
        }

    }

    private void saveSortCluster(InlongGroupInfo groupInfo, StreamSinkEntity sinkEntity, List<Topic> topicList,
            String backUpTag) {
        DataNodeEntity dataNodeEntity = dataNodeMapper.selectByUniqueKey(sinkEntity.getDataNodeName(),
                sinkEntity.getSinkType());
        DataNodeOperator nodeOperator = dataNodeOperatorFactory.getInstance(dataNodeEntity.getType());
        DataNodeInfo dataNodeInfo = nodeOperator.getFromEntity(dataNodeEntity);
        try {
            StreamSinkOperator operator = sinkOperatorFactory.getInstance(sinkEntity.getSinkType());
            List<String> fields = sinkFieldMapper.selectBySinkId(sinkEntity.getId()).stream().map(
                    StreamSinkFieldEntity::getFieldName).collect(Collectors.toList());
            Map<String, String> params = operator.parse2IdParams(sinkEntity, fields, dataNodeInfo);
            SortConfigEntity sortConfigEntity = sortConfigEntityMapper.selectBySinkId(sinkEntity.getId());
            String clusterTags = groupInfo.getInlongClusterTag();
            if (StringUtils.isNotBlank(backUpTag)) {
                clusterTags = clusterTags + InlongConstants.COMMA + backUpTag;
            }
            if (sortConfigEntity == null) {
                sortConfigEntity = CommonBeanUtils.copyProperties(sinkEntity, SortConfigEntity::new);
                sortConfigEntity.setSinkId(sinkEntity.getId());
                sortConfigEntity.setSourceParams(objectMapper.writeValueAsString(topicList));
                sortConfigEntity.setClusterParams(objectMapper.writeValueAsString(params));
                sortConfigEntity.setInlongClusterTag(clusterTags);
                sortConfigEntityMapper.insert(sortConfigEntity);
            } else {
                sortConfigEntity.setInlongClusterName(sinkEntity.getInlongClusterName());
                sortConfigEntity.setSortTaskName(sinkEntity.getSortTaskName());
                sortConfigEntity.setDataNodeName(sinkEntity.getDataNodeName());
                sortConfigEntity.setSourceParams(objectMapper.writeValueAsString(topicList));
                sortConfigEntity.setClusterParams(objectMapper.writeValueAsString(params));
                sortConfigEntity.setInlongClusterTag(clusterTags);
                sortConfigEntityMapper.updateByIdSelective(sortConfigEntity);
            }
        } catch (Exception e) {
            LOGGER.error("fail to parse id params of groupId={}, streamId={} name={}, type={}",
                    sinkEntity.getInlongGroupId(), sinkEntity.getInlongStreamId(),
                    sinkEntity.getSinkName(), sinkEntity.getSinkType(), e);
            throw new BusinessException("failed to save sort config for sinkId" + sinkEntity.getId());

        }
    }

    private Topic saveTopic(SortSourceGroupInfo sortGroupInfo, InlongStreamEntity streamEntity,
            SortSourceStreamSinkInfo sortSink) {
        try {
            List<InlongClusterEntity> clusterEntityList = clusterMapper.selectByKey(sortGroupInfo.getClusterTag(), null,
                    ClusterType.PULSAR);
            SortSourceClusterInfo cluster =
                    CommonBeanUtils.copyProperties(clusterEntityList.get(0), SortSourceClusterInfo::new);
            Map<String, String> param = cluster.getExtParamsMap();
            String namespace = sortGroupInfo.getMqResource();
            String topic = streamEntity.getMqResource();
            Map<String, String> groupExt = sortGroupInfo.getExtParamsMap();
            String groupTenant = Optional.ofNullable(groupExt.get(KEY_TENANT_V2)).orElse(groupExt.get(KEY_TENANT));
            String tenant = StringUtils.isNotBlank(groupTenant)
                    ? groupTenant
                    : Optional.ofNullable(param.get(KEY_TENANT_V2)).orElse(param.get(KEY_TENANT));
            String fullTopic = tenant + InlongConstants.SLASH + namespace + InlongConstants.SLASH + topic;

            return Topic.builder()
                    .topic(fullTopic)
                    .topicProperties(sortSink.getExtParamsMap())
                    .build();
        } catch (Exception e) {
            LOGGER.error("fail to parse topic of groupId={}, streamId={}", streamEntity.getInlongGroupId(),
                    streamEntity.getInlongStreamId(), e);
            throw new BusinessException(
                    String.format("failed to save topic for groupId=%s, streamId=%s", streamEntity.getInlongGroupId(),
                            streamEntity.getInlongStreamId()));
        }
    }
}
