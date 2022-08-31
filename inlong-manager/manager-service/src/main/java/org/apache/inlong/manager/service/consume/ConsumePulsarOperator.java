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

package org.apache.inlong.manager.service.consume;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ConsumeStatus;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongConsumeEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.consume.InlongConsumeInfo;
import org.apache.inlong.manager.pojo.consume.InlongConsumeRequest;
import org.apache.inlong.manager.pojo.consume.pulsar.ConsumePulsarDTO;
import org.apache.inlong.manager.pojo.consume.pulsar.ConsumePulsarInfo;
import org.apache.inlong.manager.pojo.consume.pulsar.ConsumePulsarRequest;
import org.apache.inlong.manager.pojo.group.InlongGroupTopicInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamBriefInfo;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.stream.InlongStreamService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Inlong consume operator for Pulsar.
 */
@Service
public class ConsumePulsarOperator extends AbstractConsumeOperator {

    private static final String PREFIX_DLQ = "dlq"; // prefix of the Topic of the dead letter queue

    private static final String PREFIX_RLQ = "rlq"; // prefix of the Topic of the retry letter queue

    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String mqType) {
        return getMQType().equals(mqType) || MQType.TDMQ_PULSAR.equals(mqType);
    }

    @Override
    public String getMQType() {
        return MQType.PULSAR;
    }

    @Override
    public void setTopicInfo(InlongConsumeRequest consumeRequest) {
        // Determine whether the consumed topic belongs to this groupId or the inlong stream under it
        String groupId = consumeRequest.getInlongGroupId();
        InlongGroupTopicInfo topicVO = groupService.getTopic(groupId);
        Preconditions.checkNotNull(topicVO, "inlong group not exist: " + groupId);

        // Pulsar's topic is the inlong stream level.
        // There will be multiple inlong streams under one inlong group, and there will be multiple topics
        List<InlongStreamBriefInfo> streamTopics = topicVO.getStreamTopics();
        if (streamTopics != null && streamTopics.size() > 0) {
            Set<String> topicSet = new HashSet<>(Arrays.asList(consumeRequest.getTopic().split(",")));
            streamTopics.forEach(stream -> topicSet.remove(stream.getMqResource()));
            Preconditions.checkEmpty(topicSet, "topic [" + topicSet + "] not belong to inlong group " + groupId);
        }
        InlongGroupEntity inlongGroupEntity = groupMapper.selectByGroupId(groupId);
        if (null != inlongGroupEntity) {
            PulsarClusterInfo pulsarCluster = (PulsarClusterInfo) clusterService.getOne(
                    inlongGroupEntity.getInlongClusterTag(), null, ClusterType.PULSAR);
            String tenant = StringUtils.isEmpty(pulsarCluster.getTenant())
                    ? InlongConstants.DEFAULT_PULSAR_TENANT : pulsarCluster.getTenant();
            consumeRequest.setTopic(String.format(InlongConstants.PULSAR_TOPIC_FORMAT, tenant,
                    inlongGroupEntity.getMqResource(), consumeRequest.getTopic()));
        }

        consumeRequest.setMqType(topicVO.getMqType());
    }

    @Override
    public InlongConsumeInfo getFromEntity(InlongConsumeEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.CONSUMER_NOR_FOUND);
        }

        ConsumePulsarInfo consumeInfo = new ConsumePulsarInfo();
        CommonBeanUtils.copyProperties(entity, consumeInfo);

        if (StringUtils.isNotBlank(entity.getExtParams())) {
            ConsumePulsarDTO dto = ConsumePulsarDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, consumeInfo);
        }
        return consumeInfo;
    }

    @Override
    protected void setExtParam(InlongConsumeRequest consumeRequest, InlongConsumeEntity entity) {
        Preconditions.checkNotNull(consumeRequest, "Pulsar info cannot be empty, as the middleware is Pulsar");
        // If it is transmitted from the web without specifying consumption pulsar info,

        ConsumePulsarRequest pulsarRequest = (ConsumePulsarRequest) consumeRequest;

        // Prerequisite for RLQ to be turned on: DLQ must be turned on
        boolean dlqEnable = (pulsarRequest.getIsDlq() != null && pulsarRequest.getIsDlq() == 1);
        boolean rlqEnable = (pulsarRequest.getIsRlq() != null && pulsarRequest.getIsRlq() == 1);
        if (rlqEnable && !dlqEnable) {
            throw new BusinessException(ErrorCodeEnum.PULSAR_DLQ_RLQ_ERROR);
        }

        // When saving, the DLQ / RLQ under the same groupId cannot be repeated;
        // when closing, delete the related configuration
        String groupId = entity.getInlongGroupId();
        if (dlqEnable) {
            String dlqTopic = PREFIX_DLQ + "_" + pulsarRequest.getDeadLetterTopic();
            Boolean exist = streamService.exist(groupId, dlqTopic);
            if (exist) {
                throw new BusinessException(ErrorCodeEnum.PULSAR_DLQ_DUPLICATED);
            }
        } else {
            pulsarRequest.setIsDlq(0);
            pulsarRequest.setDeadLetterTopic(null);
        }
        if (rlqEnable) {
            String rlqTopic = PREFIX_RLQ + "_" + pulsarRequest.getRetryLetterTopic();
            Boolean exist = streamService.exist(groupId, rlqTopic);
            if (exist) {
                throw new BusinessException(ErrorCodeEnum.PULSAR_RLQ_DUPLICATED);
            }
        } else {
            pulsarRequest.setIsRlq(0);
            pulsarRequest.setRetryLetterTopic(null);
        }

        pulsarRequest.setIsDeleted(0);
        ConsumePulsarDTO pulsarDTO = ConsumePulsarDTO.getFromRequest(pulsarRequest);

        try {
            entity.setExtParams(objectMapper.writeValueAsString(pulsarDTO));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
    }

    @Override
    protected void updateExtParam(InlongConsumeRequest consumeRequest, InlongConsumeEntity exists, String operator) {
        ConsumePulsarRequest pulsarRequest = (ConsumePulsarRequest) consumeRequest;

        // Whether DLQ / RLQ is turned on or off
        String extParams = exists.getExtParams();
        Preconditions.checkEmpty(extParams, "Pulsar consumption cannot be null");
        ConsumePulsarDTO pulsarDTO = ConsumePulsarDTO.getFromJson(extParams);

        boolean dlqEnable = (pulsarRequest.getIsDlq() != null && pulsarRequest.getIsDlq() == 1);
        boolean rlqEnable = (pulsarRequest.getIsRlq() != null && pulsarRequest.getIsRlq() == 1);

        // DLQ is closed, RLQ cannot exist alone and must be closed
        if (rlqEnable && !dlqEnable) {
            throw new BusinessException(ErrorCodeEnum.PULSAR_TOPIC_CREATE_FAILED);
        }

        // If the consumption has been approved, then close/open DLQ or RLQ, it is necessary to
        // add/remove inlong streams in the inlong group
        if (ConsumeStatus.APPROVED.getCode() == exists.getStatus()) {
            String groupId = pulsarRequest.getInlongGroupId();
            String dlqNameOld = pulsarDTO.getDeadLetterTopic();
            String dlqNameNew = pulsarRequest.getDeadLetterTopic();
            if (!dlqEnable) {
                pulsarDTO.setIsDlq(0);
                pulsarDTO.setDeadLetterTopic(null);
                streamService.logicDeleteDlqOrRlq(groupId, dlqNameOld, operator);
            } else if (!Objects.equals(dlqNameNew, dlqNameOld)) {
                pulsarDTO.setIsDlq(1);
                String topic = PREFIX_DLQ + "_" + dlqNameNew;
                topic = topic.toLowerCase(Locale.ROOT);
                pulsarDTO.setDeadLetterTopic(topic);
                streamService.insertDlqOrRlq(groupId, topic, operator);
            }

            String rlqNameOld = pulsarDTO.getRetryLetterTopic();
            String rlqNameNew = pulsarRequest.getRetryLetterTopic();
            if (!rlqEnable) {
                pulsarDTO.setIsRlq(0);
                pulsarDTO.setRetryLetterTopic(null);
                streamService.logicDeleteDlqOrRlq(groupId, rlqNameOld, operator);
            } else if (!Objects.equals(rlqNameNew, pulsarDTO.getRetryLetterTopic())) {
                pulsarDTO.setIsRlq(1);
                String topic = PREFIX_RLQ + "_" + rlqNameNew;
                topic = topic.toLowerCase(Locale.ROOT);
                pulsarDTO.setRetryLetterTopic(topic);
                streamService.insertDlqOrRlq(groupId, topic, operator);
            }
        }

        try {
            exists.setExtParams(objectMapper.writeValueAsString(pulsarDTO));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }

    }
}
