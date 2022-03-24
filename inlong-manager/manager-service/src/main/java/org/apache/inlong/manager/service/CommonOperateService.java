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

package org.apache.inlong.manager.service;

import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.pojo.dataproxy.PulsarClusterInfo;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GroupState;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamResponse;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.ThirdPartyClusterEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.ThirdPartyClusterEntityMapper;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.apache.inlong.manager.service.thirdparty.sort.util.FieldInfoUtils;
import org.apache.inlong.manager.service.thirdparty.sort.util.SinkInfoUtils;
import org.apache.inlong.manager.service.thirdparty.sort.util.SourceInfoUtils;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule.FieldMappingUnit;
import org.apache.inlong.sort.protocol.transformation.TransformationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Common operation service
 */
@Service
public class CommonOperateService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonOperateService.class);

    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private StreamSourceService streamSourceService;
    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private ThirdPartyClusterEntityMapper thirdPartyClusterMapper;

    /**
     * query some third-party-cluster info according key, such as "pulsar_adminUrl", "cluster_tube_manager", etc.
     *
     * @param key Param name.
     * @return Value of key in database.
     */
    public String getSpecifiedParam(String key) {
        String result = "";
        ThirdPartyClusterEntity clusterEntity;
        Gson gson = new Gson();
        Map<String, String> params;

        switch (key) {
            case Constant.PULSAR_SERVICEURL: {
                clusterEntity = getMQCluster(Constant.MIDDLEWARE_PULSAR);
                if (clusterEntity != null) {
                    result = clusterEntity.getUrl();
                }
                break;
            }
            case Constant.PULSAR_ADMINURL: {
                clusterEntity = getMQCluster(Constant.MIDDLEWARE_PULSAR);
                if (clusterEntity != null) {
                    params = gson.fromJson(clusterEntity.getExtParams(), Map.class);
                    result = params.get(key);
                }
                break;
            }
            case Constant.CLUSTER_TUBE_MANAGER:
            case Constant.CLUSTER_TUBE_CLUSTER_ID:
            case Constant.TUBE_MASTER_URL: {
                clusterEntity = getMQCluster(Constant.MIDDLEWARE_TUBE);
                if (clusterEntity != null) {
                    if (key.equals(Constant.TUBE_MASTER_URL)) {
                        result = clusterEntity.getUrl();
                    } else {
                        params = gson.fromJson(clusterEntity.getExtParams(), Map.class);
                        result = params.get(key);
                    }
                }
                break;
            }
        }
        return result;
    }

    /**
     * Get third party cluster by type.
     *
     * TODO Add data_proxy_cluster_name for query.
     *
     * @param type Cluster type, such as TUBE, PULSAR, etc.
     */
    private ThirdPartyClusterEntity getMQCluster(String type) {
        List<ThirdPartyClusterEntity> clusterList = thirdPartyClusterMapper.selectByType(Constant.CLUSTER_DATA_PROXY);
        if (CollectionUtils.isEmpty(clusterList)) {
            LOGGER.warn("no data proxy cluster found");
            return null;
        }
        String mqSetName = clusterList.get(0).getMqSetName();
        List<ThirdPartyClusterEntity> mqClusterList = thirdPartyClusterMapper.selectMQCluster(mqSetName,
                Collections.singletonList(type));
        if (CollectionUtils.isEmpty(mqClusterList)) {
            LOGGER.warn("no mq cluster found by type={} and mq set name={}", type, mqSetName);
            return null;
        }

        return mqClusterList.get(0);
    }

    /**
     * Get Pulsar cluster by the given type.
     *
     * @return Pulsar cluster info.
     */
    public PulsarClusterInfo getPulsarClusterInfo(String type) {
        ThirdPartyClusterEntity clusterEntity = getMQCluster(type);
        if (clusterEntity == null || StringUtils.isBlank(clusterEntity.getExtParams())) {
            throw new BusinessException("pulsar cluster or pulsar ext params is empty");
        }
        Map<String, String> configParams = JsonUtils.parse(clusterEntity.getExtParams(), Map.class);
        PulsarClusterInfo pulsarClusterInfo = PulsarClusterInfo.builder().brokerServiceUrl(
                clusterEntity.getUrl()).token(clusterEntity.getToken()).build();
        String adminUrl = configParams.get(Constant.PULSAR_ADMINURL);
        Preconditions.checkNotNull(adminUrl, "adminUrl is empty, check third party cluster table");
        pulsarClusterInfo.setAdminUrl(adminUrl);
        pulsarClusterInfo.setType(clusterEntity.getType());
        return pulsarClusterInfo;
    }

    /**
     * Check whether the inlong group status is temporary
     *
     * @param groupId Inlong group id
     * @return Inlong group entity, for caller reuse
     */
    public InlongGroupEntity checkGroupStatus(String groupId, String operator) {
        InlongGroupEntity inlongGroupEntity = groupMapper.selectByGroupId(groupId);
        Preconditions.checkNotNull(inlongGroupEntity, "groupId is invalid");

        List<String> managers = Arrays.asList(inlongGroupEntity.getInCharges().split(","));
        Preconditions.checkTrue(managers.contains(operator),
                String.format(ErrorCodeEnum.USER_IS_NOT_MANAGER.getMessage(), operator, managers));

        GroupState state = GroupState.forCode(inlongGroupEntity.getStatus());
        // Add/modify/delete is not allowed under certain group state
        if (GroupState.notAllowedUpdate(state)) {
            LOGGER.error("inlong group status was not allowed to add/update/delete related info");
            throw new BusinessException(ErrorCodeEnum.OPT_NOT_ALLOWED_BY_STATUS);
        }

        return inlongGroupEntity;
    }

    /**
     * Create dataflow info for sort.
     */
    public DataFlowInfo createDataFlow(InlongGroupInfo groupInfo, SinkResponse sinkResponse) {
        String groupId = sinkResponse.getInlongGroupId();
        String streamId = sinkResponse.getInlongStreamId();
        // TODO Support all source type, include AUTO_PUSH.
        List<SourceResponse> sourceList = streamSourceService.listSource(groupId, streamId);
        if (CollectionUtils.isEmpty(sourceList)) {
            throw new WorkflowListenerException(String.format("Source not found by groupId=%s and streamId=%s",
                    groupId, streamId));
        }

        // Get all field info
        List<FieldInfo> sourceFields = new ArrayList<>();
        List<FieldInfo> sinkFields = new ArrayList<>();
        // TODO Need support hive partition field
        if (SinkType.forType(sinkResponse.getSinkType()) == SinkType.HIVE) {
            HiveSinkResponse hiveSink = (HiveSinkResponse) sinkResponse;
            String partition = hiveSink.getPrimaryPartition();
        }

        // TODO Support more than one source and one sink
        final SourceResponse sourceResponse = sourceList.get(0);
        boolean isAllMigration = SourceInfoUtils.isBinlogAllMigration(sourceResponse);

        List<FieldMappingUnit> mappingUnitList;
        InlongStreamResponse streamInfo = streamService.get(groupId, streamId);
        if (isAllMigration) {
            mappingUnitList = FieldInfoUtils.setAllMigrationFieldMapping(sourceFields, sinkFields);
        } else {
            mappingUnitList = FieldInfoUtils.createFieldInfo(streamInfo.getFieldList(),
                    sinkResponse.getFieldList(), sourceFields, sinkFields);
        }

        FieldMappingRule fieldMappingRule = new FieldMappingRule(mappingUnitList.toArray(new FieldMappingUnit[0]));

        // Get source info
        String masterAddress = getSpecifiedParam(Constant.TUBE_MASTER_URL);
        PulsarClusterInfo pulsarCluster = getPulsarClusterInfo(groupInfo.getMiddlewareType());
        SourceInfo sourceInfo = SourceInfoUtils.createSourceInfo(pulsarCluster, masterAddress, clusterBean,
                groupInfo, streamInfo, sourceResponse, sourceFields);

        // Get sink info
        SinkInfo sinkInfo = SinkInfoUtils.createSinkInfo(sourceResponse, sinkResponse, sinkFields);

        // Get transformation info
        TransformationInfo transInfo = new TransformationInfo(fieldMappingRule);

        // Get properties
        Map<String, Object> properties = new HashMap<>();
        if (MapUtils.isNotEmpty(sinkResponse.getProperties())) {
            properties.putAll(sinkResponse.getProperties());
        }
        properties.put(Constant.DATA_FLOW_GROUP_ID_KEY, groupId);

        return new DataFlowInfo(sinkResponse.getId(), sourceInfo, transInfo, sinkInfo, properties);
    }

}
