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

package org.apache.inlong.manager.service.group;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.pojo.group.InlongGroupTopicInfo;
import org.apache.inlong.manager.pojo.group.kafka.InlongKafkaDTO;
import org.apache.inlong.manager.pojo.group.kafka.InlongKafkaInfo;
import org.apache.inlong.manager.pojo.group.kafka.InlongKafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Inlong group operator for Kafka.
 */
@Service
public class InlongKafkaOperator extends AbstractGroupOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(InlongKafkaOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String mqType) {
        return getMQType().equals(mqType);
    }

    @Override
    public String getMQType() {
        return MQType.KAFKA;
    }

    @Override
    public InlongGroupInfo getFromEntity(InlongGroupEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }

        InlongKafkaInfo kafkaInfo = new InlongKafkaInfo();
        CommonBeanUtils.copyProperties(entity, kafkaInfo);

        if (StringUtils.isNotBlank(entity.getExtParams())) {
            InlongKafkaDTO dto = InlongKafkaDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, kafkaInfo);
        }

        return kafkaInfo;
    }

    @Override
    protected void setTargetEntity(InlongGroupRequest request, InlongGroupEntity targetEntity) {
        InlongKafkaRequest kafkaRequest = (InlongKafkaRequest) request;
        CommonBeanUtils.copyProperties(kafkaRequest, targetEntity, true);
        try {
            InlongKafkaDTO dto = InlongKafkaDTO.getFromRequest(kafkaRequest);
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
        LOGGER.info("success set entity for inlong group with Kafka");
    }

    @Override
    public InlongGroupTopicInfo getTopic(InlongGroupInfo groupInfo) {
        InlongGroupTopicInfo topicInfo = super.getTopic(groupInfo);
        return topicInfo;
    }

}
