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

package org.apache.inlong.manager.service.sort.util;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.inlong.common.pojo.dataproxy.PulsarClusterInfo;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.service.CommonOperateService;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule.FieldMappingUnit;
import org.apache.inlong.sort.protocol.transformation.TransformationInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Util for build data flow info.
 */
@Service
public class DataFlowUtils {

    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private CommonOperateService commonOperateService;
    @Autowired
    private StreamSourceService streamSourceService;
    @Autowired
    private InlongStreamService streamService;

    /**
     * Create dataflow info for sort.
     */
    public DataFlowInfo createDataFlow(InlongGroupInfo groupInfo, SinkResponse sinkResponse) {
        String groupId = sinkResponse.getInlongGroupId();
        String streamId = sinkResponse.getInlongStreamId();
        List<SourceResponse> sourceList = streamSourceService.listSource(groupId, streamId);
        if (CollectionUtils.isEmpty(sourceList)) {
            throw new WorkflowListenerException(String.format("Source not found by groupId=%s and streamId=%s",
                    groupId, streamId));
        }

        // Get all field info
        List<FieldInfo> sourceFields = new ArrayList<>();
        List<FieldInfo> sinkFields = new ArrayList<>();

        // TODO Support more than one source and one sink
        final SourceResponse sourceResponse = sourceList.get(0);
        boolean isAllMigration = SourceInfoUtils.isBinlogAllMigration(sourceResponse);

        List<FieldMappingUnit> mappingUnitList;
        InlongStreamInfo streamInfo = streamService.get(groupId, streamId);
        if (isAllMigration) {
            mappingUnitList = FieldInfoUtils.setAllMigrationFieldMapping(sourceFields, sinkFields);
        } else {
            mappingUnitList = FieldInfoUtils.createFieldInfo(streamInfo.getFieldList(),
                    sinkResponse.getFieldList(), sourceFields, sinkFields);
        }

        FieldMappingRule fieldMappingRule = new FieldMappingRule(mappingUnitList.toArray(new FieldMappingUnit[0]));

        // Get source info
        String masterAddress = commonOperateService.getSpecifiedParam(InlongGroupSettings.TUBE_MASTER_URL);
        PulsarClusterInfo pulsarCluster = commonOperateService.getPulsarClusterInfo(groupInfo.getMqType());
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
        properties.put(InlongGroupSettings.DATA_FLOW_GROUP_ID_KEY, groupId);

        return new DataFlowInfo(sinkResponse.getId(), sourceInfo, transInfo, sinkInfo, properties);
    }

}
