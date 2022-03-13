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

package org.apache.inlong.manager.service.thirdparty.sort.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.common.pojo.dataproxy.PulsarClusterInfo;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPulsarInfo;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import org.apache.inlong.sort.protocol.source.PulsarSourceInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;
import org.apache.inlong.sort.protocol.source.TDMQPulsarSourceInfo;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;

import java.util.List;

/**
 * Utils for source info
 */
public class SourceInfoUtils {

    /**
     * Whether the source is all binlog migration.
     */
    public static boolean isAllMigration(SourceResponse sourceResponse) {
        if (sourceResponse == null) {
            return false;
        }
        if (SourceType.BINLOG.getType().equalsIgnoreCase(sourceResponse.getSourceType())) {
            BinlogSourceResponse binlogSource = (BinlogSourceResponse) sourceResponse;
            return binlogSource.isAllMigration();
        }
        if (SourceType.KAFKA.getType().equalsIgnoreCase(sourceResponse.getSourceType())) {
            KafkaSourceResponse kafkaSourceResponse = (KafkaSourceResponse) sourceResponse;
            if (DataTypeEnum.forName(kafkaSourceResponse.getSerializationType()) == DataTypeEnum.CANAL) {
                return true;
            }
        }
        return false;
    }

    /**
     * Create source info for DataFlowInfo.
     */
    public static SourceInfo createSourceInfo(PulsarClusterInfo pulsarCluster, String masterAddress,
            ClusterBean clusterBean, InlongGroupInfo groupInfo, InlongStreamInfo streamInfo,
            SourceResponse sourceResponse, List<FieldInfo> sourceFields) {

        String mqType = groupInfo.getMiddlewareType();
        DeserializationInfo deserializationInfo = SerializationUtils.createDeserialInfo(sourceResponse, streamInfo);
        SourceInfo sourceInfo;
        if (Constant.MIDDLEWARE_PULSAR.equals(mqType) || Constant.MIDDLEWARE_TDMQ_PULSAR.equals(mqType)) {
            sourceInfo = createPulsarSourceInfo(pulsarCluster, clusterBean, groupInfo, streamInfo, deserializationInfo,
                    sourceFields);
        } else if (Constant.MIDDLEWARE_TUBE.equals(mqType)) {
            // InlongGroupInfo groupInfo, String masterAddress,
            sourceInfo = createTubeSourceInfo(groupInfo, masterAddress, clusterBean, deserializationInfo, sourceFields);
        } else {
            throw new WorkflowListenerException(String.format("Unsupported middleware {%s}", mqType));
        }

        return sourceInfo;
    }

    /**
     * Create source info for Pulsar
     */
    private static SourceInfo createPulsarSourceInfo(PulsarClusterInfo pulsarCluster, ClusterBean clusterBean,
            InlongGroupInfo groupInfo, InlongStreamInfo streamInfo,
            DeserializationInfo deserializationInfo, List<FieldInfo> fieldInfos) {
        String topicName = streamInfo.getMqResourceObj();
        InlongGroupPulsarInfo pulsarInfo = (InlongGroupPulsarInfo) groupInfo.getMqExtInfo();
        String tenant = clusterBean.getDefaultTenant();
        if (StringUtils.isNotEmpty(pulsarInfo.getTenant())) {
            tenant = pulsarInfo.getTenant();
        }

        final String namespace = groupInfo.getMqResourceObj();
        // Full name of topic in Pulsar
        final String fullTopicName = "persistent://" + tenant + "/" + namespace + "/" + topicName;
        final String consumerGroup = clusterBean.getAppName() + "_" + topicName + "_consumer_group";
        FieldInfo[] fieldInfosArr = fieldInfos.toArray(new FieldInfo[0]);

        String type = pulsarCluster.getType();
        if (StringUtils.isNotEmpty(type) && Constant.MIDDLEWARE_TDMQ_PULSAR.equals(type)) {
            return new TDMQPulsarSourceInfo(pulsarCluster.getBrokerServiceUrl(),
                    fullTopicName, consumerGroup, pulsarCluster.getToken(), deserializationInfo, fieldInfosArr);
        } else {
            return new PulsarSourceInfo(pulsarCluster.getAdminUrl(), pulsarCluster.getBrokerServiceUrl(),
                    fullTopicName, consumerGroup, deserializationInfo, fieldInfosArr, pulsarCluster.getToken());
        }
    }

    /**
     * Create source info TubeMQ
     */
    private static TubeSourceInfo createTubeSourceInfo(InlongGroupInfo groupInfo, String masterAddress,
            ClusterBean clusterBean, DeserializationInfo deserializationInfo, List<FieldInfo> fieldInfos) {
        Preconditions.checkNotNull(masterAddress, "tube cluster address cannot be empty");
        String topic = groupInfo.getMqResourceObj();
        String consumerGroup = clusterBean.getAppName() + "_" + topic + "_consumer_group";
        return new TubeSourceInfo(topic, masterAddress, consumerGroup, deserializationInfo,
                fieldInfos.toArray(new FieldInfo[0]));
    }

}
