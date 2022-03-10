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

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.pojo.dataproxy.PulsarClusterInfo;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPulsarInfo;
import org.apache.inlong.manager.common.pojo.sink.SinkFieldResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo.BuiltInField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import org.apache.inlong.sort.protocol.source.PulsarSourceInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;
import org.apache.inlong.sort.protocol.source.TDMQPulsarSourceInfo;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Utils for source info
 */
public class SourceInfoUtils {

    /**
     * Built in field map, key is field name, value is built in field name
     */
    public static final Map<String, BuiltInField> BUILT_IN_FIELD_MAP = new HashMap<>();

    static {
        BUILT_IN_FIELD_MAP.put("data_time", BuiltInField.DATA_TIME);
        BUILT_IN_FIELD_MAP.put("database", BuiltInField.MYSQL_METADATA_DATABASE);
        BUILT_IN_FIELD_MAP.put("table", BuiltInField.MYSQL_METADATA_TABLE);
        BUILT_IN_FIELD_MAP.put("event_time", BuiltInField.MYSQL_METADATA_EVENT_TIME);
        BUILT_IN_FIELD_MAP.put("is_ddl", BuiltInField.MYSQL_METADATA_IS_DDL);
        BUILT_IN_FIELD_MAP.put("event_type", BuiltInField.MYSQL_METADATA_EVENT_TYPE);
    }

    /**
     * Whether the source is all binlog migration.
     */
    public static boolean isBinlogAllMigration(SourceResponse sourceResponse) {
        if (SourceType.BINLOG.getType().equalsIgnoreCase(sourceResponse.getSourceType())) {
            BinlogSourceResponse binlogSourceResponse = (BinlogSourceResponse) sourceResponse;
            return binlogSourceResponse.isAllMigration();
        }
        return false;
    }

    /**
     * Get all migration built-in field for binlog source.
     */
    public static List<FieldInfo> getAllMigrationBuiltInField() {
        List<FieldInfo> list = Lists.newArrayList();
        list.add(new BuiltInFieldInfo("data", StringFormatInfo.INSTANCE,
                BuiltInField.MYSQL_METADATA_DATA));
        for (Map.Entry<String, BuiltInField> entry : BUILT_IN_FIELD_MAP.entrySet()) {
            list.add(new BuiltInFieldInfo(entry.getKey(), StringFormatInfo.INSTANCE, entry.getValue()));
        }
        return list;
    }

    /**
     * Create source info for DataFlowInfo.
     */
    public static SourceInfo createSourceInfo(PulsarClusterInfo pulsarCluster, String masterAddress,
            ClusterBean clusterBean, InlongGroupInfo groupInfo, InlongStreamInfo streamInfo,
            SourceResponse sourceResponse, SinkResponse sinkResponse) {

        List<FieldInfo> fieldInfos = Lists.newArrayList();
        boolean isAllMigration = SourceInfoUtils.isBinlogAllMigration(sourceResponse);
        if (isAllMigration) {
            fieldInfos = SourceInfoUtils.getAllMigrationBuiltInField();
        } else {
            if (CollectionUtils.isNotEmpty(streamInfo.getFieldList())) {
                fieldInfos = getSourceFields(sinkResponse.getFieldList());
            }
        }

        String middleWareType = groupInfo.getMiddlewareType();
        DeserializationInfo deserializationInfo = SerializationUtils.createDeserializationInfo(sourceResponse,
                streamInfo);
        SourceInfo sourceInfo;
        if (Constant.MIDDLEWARE_PULSAR.equals(middleWareType)) {
            sourceInfo = createPulsarSourceInfo(pulsarCluster, clusterBean, groupInfo, streamInfo, deserializationInfo,
                    fieldInfos);
        } else if (Constant.MIDDLEWARE_TUBE.equals(middleWareType)) {
            // InlongGroupInfo groupInfo, String masterAddress,
            sourceInfo = createTubeSourceInfo(groupInfo, masterAddress, clusterBean, deserializationInfo, fieldInfos);
        } else {
            throw new WorkflowListenerException(String.format("Unsupported middleware {%s}", middleWareType));
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
        if (StringUtils.isNotEmpty(type) && type.toUpperCase(Locale.ROOT).contains(Constant.MIDDLEWARE_TDMQ)) {
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

    /**
     * Get source field list.
     *
     * TODO 1. Support partition field, 2. Add is_metadata field in StreamSinkFieldEntity
     */
    private static List<FieldInfo> getSourceFields(List<SinkFieldResponse> fieldList) {
        List<FieldInfo> fieldInfoList = new ArrayList<>();
        for (SinkFieldResponse field : fieldList) {
            FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(field.getSourceFieldType().toLowerCase());
            String fieldName = field.getSourceFieldName();

            FieldInfo fieldInfo;
            // If the field name equals to build-in field, new a build-in field info
            BuiltInField builtInField = SourceInfoUtils.BUILT_IN_FIELD_MAP.get(fieldName);
            if (builtInField == null) {
                fieldInfo = new FieldInfo(fieldName, formatInfo);
            } else {
                fieldInfo = new BuiltInFieldInfo(fieldName, formatInfo, builtInField);
            }
            fieldInfoList.add(fieldInfo);
        }

        return fieldInfoList;
    }

}
