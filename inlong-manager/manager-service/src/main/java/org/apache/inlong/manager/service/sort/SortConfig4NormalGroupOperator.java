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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.tube.TubeClusterInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.sink.StreamSink;
import org.apache.inlong.manager.common.pojo.source.StreamSource;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSource;
import org.apache.inlong.manager.common.pojo.source.pulsar.PulsarSource;
import org.apache.inlong.manager.common.pojo.source.tubemq.TubeMQSource;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamExtInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.sort.util.ExtractNodeUtils;
import org.apache.inlong.manager.service.sort.util.LoadNodeUtils;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.apache.inlong.sort.protocol.GroupInfo;
import org.apache.inlong.sort.protocol.StreamInfo;
import org.apache.inlong.sort.protocol.enums.PulsarScanStartupMode;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Sort config operator, used to create a Sort config for the InlongGroup in normal mode with ZK disabled.
 */
@Service
public class SortConfig4NormalGroupOperator implements SortConfigOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortConfig4NormalGroupOperator.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    private StreamSourceService sourceService;
    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InlongClusterService clusterService;

    @Override
    public Boolean accept(Integer isNormal, Integer enableZk) {
        return InlongConstants.NORMAL_MODE.equals(isNormal)
                && InlongConstants.DISABLE_ZK.equals(enableZk);
    }

    @Override
    public void buildConfig(InlongGroupInfo groupInfo, List<InlongStreamInfo> streamInfos, boolean isStream)
            throws Exception {
        if (groupInfo == null || CollectionUtils.isEmpty(streamInfos)) {
            LOGGER.warn("group info is null or stream infos is empty, no need to build sort config for disable zk");
            return;
        }

        GroupInfo configInfo = this.createSortGroupInfo(groupInfo, streamInfos);
        String dataflow = OBJECT_MAPPER.writeValueAsString(configInfo);

        if (isStream) {
            this.addToStreamExt(streamInfos, dataflow);
        } else {
            this.addToGroupExt(groupInfo, dataflow);
        }
    }

    /**
     * Create GroupInfo for Sort protocol.
     *
     * @see org.apache.inlong.sort.protocol.GroupInfo
     */
    private GroupInfo createSortGroupInfo(InlongGroupInfo groupInfo, List<InlongStreamInfo> streamInfoList) {
        String groupId = groupInfo.getInlongGroupId();
        List<StreamSink> streamSinks = sinkService.listSink(groupId, null);
        Map<String, List<StreamSink>> sinkMap = streamSinks.stream()
                .collect(Collectors.groupingBy(StreamSink::getInlongStreamId, HashMap::new,
                        Collectors.toCollection(ArrayList::new)));

        // get source info
        Map<String, List<StreamSource>> sourceMap;
        MQType.forType(groupInfo.getMqType());
        sourceMap = createMQSources(groupInfo, streamInfoList);
        // create StreamInfo for Sort protocol
        List<StreamInfo> sortStreamInfos = new ArrayList<>();
        for (InlongStreamInfo inlongStream : streamInfoList) {
            String streamId = inlongStream.getInlongStreamId();
            List<StreamSource> sources = sourceMap.get(streamId);
            List<StreamSink> sinks = sinkMap.get(streamId);
            StreamInfo sortStream = new StreamInfo(streamId,
                    this.createNodesForStream(sources, sinks),
                    this.createNodeRelationsForStream(sources, sinks));
            sortStreamInfos.add(sortStream);
        }

        return new GroupInfo(groupId, sortStreamInfos);
    }

    private Map<String, List<StreamSource>> createMQSources(
            InlongGroupInfo groupInfo, List<InlongStreamInfo> streamInfoList) {
        if (MQType.forType(groupInfo.getMqType()) == MQType.TUBE) {
            return createTubeSource(groupInfo, streamInfoList);
        }
        return createPulsarSource(groupInfo, streamInfoList);
    }

    /**
     * Create Tube sources for Sort.
     */
    private Map<String, List<StreamSource>> createTubeSource(
            InlongGroupInfo groupInfo, List<InlongStreamInfo> streamInfoList) {
        ClusterInfo clusterInfo = clusterService.getOne(groupInfo.getInlongClusterTag(), null,
                ClusterType.TUBE);
        TubeClusterInfo tubeClusterInfo = (TubeClusterInfo) clusterInfo;
        String masterRpc = tubeClusterInfo.getUrl();
        Map<String, List<StreamSource>> sourceMap = Maps.newHashMap();
        streamInfoList.forEach(streamInfo -> {
            TubeMQSource tubeMQSource = new TubeMQSource();
            String streamId = streamInfo.getInlongStreamId();
            tubeMQSource.setSourceName(streamId);
            tubeMQSource.setTopic(streamInfo.getMqResource());
            tubeMQSource.setGroupId(streamId);
            tubeMQSource.setMasterRpc(masterRpc);
            List<StreamSource> sourceInfos = sourceService.listSource(groupInfo.getInlongGroupId(), streamId);
            for (StreamSource sourceInfo : sourceInfos) {
                tubeMQSource.setSerializationType(sourceInfo.getSerializationType());
            }
            tubeMQSource.setFieldList(streamInfo.getFieldList());
            sourceMap.computeIfAbsent(streamId, key -> Lists.newArrayList()).add(tubeMQSource);
        });

        return sourceMap;
    }

    /**
     * Create Pulsar sources for Sort.
     */
    private Map<String, List<StreamSource>> createPulsarSource(
            InlongGroupInfo groupInfo, List<InlongStreamInfo> streamInfoList) {
        ClusterInfo clusterInfo = clusterService.getOne(groupInfo.getInlongClusterTag(), null,
                ClusterType.PULSAR);
        PulsarClusterInfo pulsarCluster = (PulsarClusterInfo) clusterInfo;
        String adminUrl = pulsarCluster.getAdminUrl();
        String serviceUrl = pulsarCluster.getUrl();
        String tenant = StringUtils.isEmpty(pulsarCluster.getTenant())
                ? InlongConstants.DEFAULT_PULSAR_TENANT : pulsarCluster.getTenant();

        Map<String, List<StreamSource>> sourceMap = Maps.newHashMap();
        streamInfoList.forEach(streamInfo -> {
            PulsarSource pulsarSource = new PulsarSource();
            String streamId = streamInfo.getInlongStreamId();
            pulsarSource.setSourceName(streamId);
            pulsarSource.setTenant(tenant);
            pulsarSource.setNamespace(groupInfo.getMqResource());
            pulsarSource.setTopic(streamInfo.getMqResource());
            pulsarSource.setAdminUrl(adminUrl);
            pulsarSource.setServiceUrl(serviceUrl);
            pulsarSource.setInlongComponent(true);

            List<StreamSource> sourceInfos = sourceService.listSource(groupInfo.getInlongGroupId(), streamId);
            for (StreamSource sourceInfo : sourceInfos) {
                if (StringUtils.isEmpty(pulsarSource.getSerializationType())
                        && StringUtils.isNotEmpty(sourceInfo.getSerializationType())) {
                    pulsarSource.setSerializationType(sourceInfo.getSerializationType());
                }
                if (SourceType.forType(sourceInfo.getSourceType()) == SourceType.KAFKA) {
                    pulsarSource.setPrimaryKey(((KafkaSource) sourceInfo).getPrimaryKey());
                }
            }

            // if the SerializationType is still null, set it to the CSV
            if (StringUtils.isEmpty(pulsarSource.getSerializationType())) {
                pulsarSource.setSerializationType(DataTypeEnum.CSV.getName());
            }
            pulsarSource.setScanStartupMode(PulsarScanStartupMode.EARLIEST.getValue());
            pulsarSource.setFieldList(streamInfo.getFieldList());
            sourceMap.computeIfAbsent(streamId, key -> Lists.newArrayList()).add(pulsarSource);
        });

        return sourceMap;
    }

    private List<Node> createNodesForStream(List<StreamSource> sources, List<StreamSink> streamSinks) {
        List<Node> nodes = Lists.newArrayList();
        nodes.addAll(ExtractNodeUtils.createExtractNodes(sources));
        nodes.addAll(LoadNodeUtils.createLoadNodes(streamSinks));
        return nodes;
    }

    private List<NodeRelation> createNodeRelationsForStream(List<StreamSource> sources, List<StreamSink> streamSinks) {
        NodeRelation relation = new NodeRelation();
        List<String> inputs = sources.stream().map(StreamSource::getSourceName).collect(Collectors.toList());
        List<String> outputs = streamSinks.stream().map(StreamSink::getSinkName).collect(Collectors.toList());
        relation.setInputs(inputs);
        relation.setOutputs(outputs);
        return Lists.newArrayList(relation);
    }

    /**
     * Add config into inlong group ext info
     */
    private void addToGroupExt(InlongGroupInfo groupInfo, String value) {
        if (groupInfo.getExtList() == null) {
            groupInfo.setExtList(Lists.newArrayList());
        }

        InlongGroupExtInfo extInfo = new InlongGroupExtInfo();
        extInfo.setInlongGroupId(groupInfo.getInlongGroupId());
        extInfo.setKeyName(InlongConstants.DATAFLOW);
        extInfo.setKeyValue(value);

        groupInfo.getExtList().removeIf(ext -> extInfo.getKeyName().equals(ext.getKeyName()));
        groupInfo.getExtList().add(extInfo);
    }

    /**
     * Add config into inlong stream ext info
     */
    private void addToStreamExt(List<InlongStreamInfo> streamInfos, String value) {
        streamInfos.forEach(streamInfo -> {
            if (streamInfo.getExtList() == null) {
                streamInfo.setExtList(Lists.newArrayList());
            }

            InlongStreamExtInfo extInfo = new InlongStreamExtInfo();
            extInfo.setInlongGroupId(streamInfo.getInlongGroupId());
            extInfo.setInlongStreamId(streamInfo.getInlongStreamId());
            extInfo.setKeyName(InlongConstants.DATAFLOW);
            extInfo.setKeyValue(value);

            streamInfo.getExtList().removeIf(ext -> extInfo.getKeyName().equals(ext.getKeyName()));
            streamInfo.getExtList().add(extInfo);
        });
    }

}
