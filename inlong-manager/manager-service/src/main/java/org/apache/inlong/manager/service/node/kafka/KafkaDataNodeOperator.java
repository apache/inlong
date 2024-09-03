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

package org.apache.inlong.manager.service.node.kafka;

import org.apache.inlong.common.pojo.sort.node.KafkaNodeConfig;
import org.apache.inlong.common.pojo.sort.node.NodeConfig;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.pojo.cluster.kafka.KafkaClusterInfo;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.pojo.node.kafka.KafkaDataNodeDTO;
import org.apache.inlong.manager.pojo.node.kafka.KafkaDataNodeInfo;
import org.apache.inlong.manager.pojo.node.kafka.KafkaDataNodeRequest;
import org.apache.inlong.manager.service.node.AbstractDataNodeOperator;
import org.apache.inlong.manager.service.resource.queue.kafka.KafkaUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

/**
 * Kafka data node operator
 */
@Service
public class KafkaDataNodeOperator extends AbstractDataNodeOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDataNodeOperator.class);

    private static final String bootstrapServers = "bootstrap.servers";
    private static final String clientId = "client.id";

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RestTemplate restTemplate;

    @Override
    public Boolean accept(String dataNodeType) {
        return getDataNodeType().equals(dataNodeType);
    }

    @Override
    public String getDataNodeType() {
        return DataNodeType.KAFKA;
    }

    @Override
    public DataNodeInfo getFromEntity(DataNodeEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.DATA_NODE_NOT_FOUND);
        }

        KafkaDataNodeInfo kafkaDataNodeInfo = new KafkaDataNodeInfo();
        CommonBeanUtils.copyProperties(entity, kafkaDataNodeInfo);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            KafkaDataNodeDTO dto = KafkaDataNodeDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, kafkaDataNodeInfo);
        }
        return kafkaDataNodeInfo;
    }

    @Override
    public Map<String, String> parse2SinkParams(DataNodeInfo info) {
        Map<String, String> params = super.parse2SinkParams(info);
        KafkaDataNodeInfo kafkaDataNodeInfo = (KafkaDataNodeInfo) info;
        params.put(bootstrapServers, kafkaDataNodeInfo.getBootstrapServers());
        params.put(clientId, kafkaDataNodeInfo.getClientId());
        return params;
    }

    @Override
    protected void setTargetEntity(DataNodeRequest request, DataNodeEntity targetEntity) {
        KafkaDataNodeRequest nodeRequest = (KafkaDataNodeRequest) request;
        CommonBeanUtils.copyProperties(nodeRequest, targetEntity, true);
        try {
            KafkaDataNodeDTO dto = KafkaDataNodeDTO.getFromRequest(nodeRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.DATA_NODE_INFO_INCORRECT,
                    String.format("Failed to build extParams for kafka node: %s", e.getMessage()));
        }
    }

    @Override
    public Boolean testConnection(DataNodeRequest request) {
        KafkaDataNodeRequest kafkaDataNodeRequest = (KafkaDataNodeRequest) request;
        String bootstrapServers = kafkaDataNodeRequest.getBootstrapServers();
        Preconditions.expectNotBlank(bootstrapServers, ErrorCodeEnum.INVALID_PARAMETER,
                "connection bootstrapServers  cannot be empty");
        if (getKafkaConnection(bootstrapServers)) {
            LOGGER.info("kafka connection success for bootstrapServers={}",
                    bootstrapServers);
        }
        return true;
    }

    @Override
    public NodeConfig getNodeConfig(DataNodeEntity dataNodeEntity) {
        DataNodeInfo dataNodeInfo = this.getFromEntity(dataNodeEntity);
        KafkaNodeConfig kafkaNodeConfig = CommonBeanUtils.copyProperties(dataNodeInfo, KafkaNodeConfig::new);
        // KafkaDataNodeDTO dto = KafkaDataNodeDTO.getFromJson(dataNodeEntity.getExtParams());
        // CommonBeanUtils.copyProperties(dto, kafkaNodeConfig, true);
        kafkaNodeConfig.setNodeName(dataNodeInfo.getName());
        return kafkaNodeConfig;
    }

    private boolean getKafkaConnection(String bootstrapServers) {
        KafkaClusterInfo kafkaClusterInfo = KafkaClusterInfo.builder().bootstrapServers(bootstrapServers).build();
        try {

            // test connect for kafka adminUrl
            KafkaUtils.getAdminClient(kafkaClusterInfo);
            return true;
        } catch (Exception e) {
            String errMsg = String.format("Kafka connection failed for bootstrapServers=%s",
                    kafkaClusterInfo.getBootstrapServers());
            LOGGER.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
    }
}
