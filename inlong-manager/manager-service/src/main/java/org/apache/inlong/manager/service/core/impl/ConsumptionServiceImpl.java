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

package org.apache.inlong.manager.service.core.impl;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.ConsumptionStatus;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessTopicVO;
import org.apache.inlong.manager.common.pojo.consumption.ConsumptionInfo;
import org.apache.inlong.manager.common.pojo.consumption.ConsumptionListVo;
import org.apache.inlong.manager.common.pojo.consumption.ConsumptionMqExtBase;
import org.apache.inlong.manager.common.pojo.consumption.ConsumptionPulsarInfo;
import org.apache.inlong.manager.common.pojo.consumption.ConsumptionQuery;
import org.apache.inlong.manager.common.pojo.consumption.ConsumptionSummary;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamTopicVO;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.BusinessEntity;
import org.apache.inlong.manager.dao.entity.ConsumptionEntity;
import org.apache.inlong.manager.dao.entity.ConsumptionPulsarEntity;
import org.apache.inlong.manager.dao.mapper.BusinessEntityMapper;
import org.apache.inlong.manager.dao.mapper.ConsumptionEntityMapper;
import org.apache.inlong.manager.dao.mapper.ConsumptionPulsarEntityMapper;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.core.ConsumptionService;
import org.apache.inlong.manager.service.core.DataStreamService;
import org.apache.inlong.manager.service.workflow.ProcessName;
import org.apache.inlong.manager.service.workflow.WorkflowResult;
import org.apache.inlong.manager.service.workflow.WorkflowService;
import org.apache.inlong.manager.service.workflow.newconsumption.NewConsumptionWorkflowForm;
import org.apache.inlong.manager.workflow.model.view.CountByKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

/**
 * Data consumption service
 */
@Slf4j
@Service
public class ConsumptionServiceImpl implements ConsumptionService {

    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private BusinessEntityMapper businessMapper;
    @Autowired
    private ConsumptionEntityMapper consumptionMapper;
    @Autowired
    private ConsumptionPulsarEntityMapper consumptionPulsarMapper;
    @Autowired
    private WorkflowService workflowService;
    @Autowired
    private BusinessService businessService;
    @Autowired
    private DataStreamService streamService;

    @Override
    public ConsumptionSummary getSummary(ConsumptionQuery query) {
        Map<String, Integer> countByState = consumptionMapper.countByStatus(query)
                .stream()
                .collect(Collectors.toMap(CountByKey::getKey, CountByKey::getValue));

        return ConsumptionSummary.builder()
                .totalCount(countByState.values().stream().mapToInt(c -> c).sum())
                .waitingAssignCount(countByState.getOrDefault(ConsumptionStatus.WAITING_ASSIGN.getStatus() + "", 0))
                .waitingApproveCount(countByState.getOrDefault(ConsumptionStatus.WAITING_APPROVE.getStatus() + "", 0))
                .rejectedCount(countByState.getOrDefault(ConsumptionStatus.REJECTED.getStatus() + "", 0)).build();
    }

    @Override
    public PageInfo<ConsumptionListVo> list(ConsumptionQuery query) {
        PageHelper.startPage(query.getPageNum(), query.getPageSize());
        Page<ConsumptionEntity> pageResult = (Page<ConsumptionEntity>) consumptionMapper.listByQuery(query);
        PageInfo<ConsumptionListVo> pageInfo = pageResult
                .toPageInfo(entity -> CommonBeanUtils.copyProperties(entity, ConsumptionListVo::new));
        pageInfo.setTotal(pageResult.getTotal());
        return pageInfo;
    }

    @Override
    public ConsumptionInfo get(Integer id) {
        Preconditions.checkNotNull(id, "consumption id cannot be null");
        ConsumptionEntity entity = consumptionMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "consumption not exist with id:" + id);

        ConsumptionInfo info = CommonBeanUtils.copyProperties(entity, ConsumptionInfo::new);

        if (BizConstant.MIDDLEWARE_PULSAR.equalsIgnoreCase(info.getMiddlewareType())) {
            ConsumptionPulsarEntity pulsarEntity = consumptionPulsarMapper.selectByConsumptionId(info.getId());
            Preconditions.checkNotNull(pulsarEntity, "Pulsar consumption cannot be empty, as the middleware is Pulsar");
            ConsumptionPulsarInfo pulsarInfo = CommonBeanUtils.copyProperties(pulsarEntity, ConsumptionPulsarInfo::new);
            info.setMqExtInfo(pulsarInfo);

            info.setTopic(this.getFullPulsarTopic(info.getInlongGroupId(), info.getTopic()));
        }

        return info;
    }

    @Override
    public boolean isConsumerGroupIdExists(String consumerGroup, Integer excludeSelfId) {
        ConsumptionQuery consumptionQuery = new ConsumptionQuery();
        consumptionQuery.setConsumerGroupId(consumerGroup);
        List<ConsumptionEntity> result = consumptionMapper.listByQuery(consumptionQuery);
        if (excludeSelfId != null) {
            result = result.stream().filter(consumer -> !excludeSelfId.equals(consumer.getId()))
                    .collect(Collectors.toList());
        }
        return !CollectionUtils.isEmpty(result);
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Integer save(ConsumptionInfo info, String operator) {
        fullConsumptionInfo(info);

        Date now = new Date();
        ConsumptionEntity entity = this.saveConsumption(info, operator, now);

        if (BizConstant.MIDDLEWARE_PULSAR.equals(entity.getMiddlewareType())) {
            savePulsarInfo(info.getMqExtInfo(), entity);
        }

        return entity.getId();
    }

    /**
     * Save Pulsar consumption info
     */
    private void savePulsarInfo(ConsumptionMqExtBase mqExtBase, ConsumptionEntity entity) {
        Preconditions.checkNotNull(mqExtBase, "Pulsar info cannot be empty, as the middleware is Pulsar");
        ConsumptionPulsarInfo pulsarInfo = (ConsumptionPulsarInfo) mqExtBase;

        // Prerequisite for RLQ to be turned on: DLQ must be turned on
        boolean dlqEnable = (pulsarInfo.getIsDlq() != null && pulsarInfo.getIsDlq() == 1);
        boolean rlqEnable = (pulsarInfo.getIsRlq() != null && pulsarInfo.getIsRlq() == 1);
        if (rlqEnable && !dlqEnable) {
            throw new BusinessException(BizErrorCodeEnum.PULSAR_DLQ_RLQ_ERROR);
        }

        // When saving, the DLQ / RLQ under the same groupId cannot be repeated;
        // when closing, delete the related configuration
        String groupId = entity.getInlongGroupId();
        if (dlqEnable) {
            String dlqTopic = BizConstant.PREFIX_DLQ + "_" + pulsarInfo.getDeadLetterTopic();
            Boolean exist = streamService.exist(groupId, dlqTopic);
            if (exist) {
                throw new BusinessException(BizErrorCodeEnum.PULSAR_DLQ_DUPLICATED);
            }
        } else {
            pulsarInfo.setIsDlq(0);
            pulsarInfo.setDeadLetterTopic(null);
        }
        if (rlqEnable) {
            String rlqTopic = BizConstant.PREFIX_RLQ + "_" + pulsarInfo.getRetryLetterTopic();
            Boolean exist = streamService.exist(groupId, rlqTopic);
            if (exist) {
                throw new BusinessException(BizErrorCodeEnum.PULSAR_RLQ_DUPLICATED);
            }
        } else {
            pulsarInfo.setIsRlq(0);
            pulsarInfo.setRetryLetterTopic(null);
        }

        ConsumptionPulsarEntity pulsar = CommonBeanUtils.copyProperties(pulsarInfo, ConsumptionPulsarEntity::new);
        Integer consumptionId = entity.getId();
        pulsar.setConsumptionId(consumptionId);
        pulsar.setInlongGroupId(groupId);
        pulsar.setConsumerGroupId(entity.getConsumerGroupId());
        pulsar.setIsDeleted(0);

        // Pulsar consumer information may already exist, update if it exists, add if it does not exist
        ConsumptionPulsarEntity exists = consumptionPulsarMapper.selectByConsumptionId(consumptionId);
        if (exists == null) {
            consumptionPulsarMapper.insert(pulsar);
        } else {
            pulsar.setId(exists.getId());
            consumptionPulsarMapper.updateByPrimaryKey(pulsar);
        }
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Boolean update(ConsumptionInfo info, String operator) {
        Preconditions.checkNotNull(info, "consumption info cannot be null");
        Integer consumptionId = info.getId();
        Preconditions.checkNotNull(consumptionId, "consumption id cannot be null");

        ConsumptionEntity exists = consumptionMapper.selectByPrimaryKey(consumptionId);
        Preconditions.checkNotNull(exists, "consumption not exist with id " + consumptionId);
        Preconditions.checkTrue(exists.getInCharges().contains(operator),
                "operator" + operator + " has no privilege for the consumption");

        ConsumptionEntity entity = new ConsumptionEntity();
        Date now = new Date();
        CommonBeanUtils.copyProperties(info, entity, true);
        entity.setModifier(operator);
        entity.setModifyTime(now);

        // Modify Pulsar consumption info
        if (BizConstant.MIDDLEWARE_PULSAR.equalsIgnoreCase(info.getMiddlewareType())) {
            ConsumptionPulsarEntity pulsarEntity = consumptionPulsarMapper.selectByConsumptionId(consumptionId);
            Preconditions.checkNotNull(pulsarEntity, "Pulsar consumption cannot be null");
            pulsarEntity.setConsumerGroupId(info.getConsumerGroupId());

            // Whether DLQ / RLQ is turned on or off
            ConsumptionPulsarInfo update = (ConsumptionPulsarInfo) info.getMqExtInfo();
            boolean dlqEnable = (update.getIsDlq() != null && update.getIsDlq() == 1);
            boolean rlqEnable = (update.getIsRlq() != null && update.getIsRlq() == 1);

            // DLQ is closed, RLQ cannot exist alone and must be closed
            if (rlqEnable && !dlqEnable) {
                throw new BusinessException(BizErrorCodeEnum.PULSAR_TOPIC_CREATE_FAILED);
            }

            // If the consumption has been approved, then close/open DLQ or RLQ, it is necessary to
            // add/remove data streams in the business
            if (ConsumptionStatus.APPROVED.getStatus() == exists.getStatus()) {
                String groupId = info.getInlongGroupId();
                String dlqNameOld = pulsarEntity.getDeadLetterTopic();
                String dlqNameNew = update.getDeadLetterTopic();
                if (!dlqEnable) {
                    pulsarEntity.setIsDlq(0);
                    pulsarEntity.setDeadLetterTopic(null);
                    streamService.logicDeleteDlqOrRlq(groupId, dlqNameOld, operator);
                } else if (!Objects.equals(dlqNameNew, dlqNameOld)) {
                    pulsarEntity.setIsDlq(1);
                    String topic = BizConstant.PREFIX_DLQ + "_" + dlqNameNew;
                    topic = topic.toLowerCase(Locale.ROOT);
                    pulsarEntity.setDeadLetterTopic(topic);
                    streamService.insertDlqOrRlq(groupId, topic, operator);
                }

                String rlqNameOld = pulsarEntity.getRetryLetterTopic();
                String rlqNameNew = update.getRetryLetterTopic();
                if (!rlqEnable) {
                    pulsarEntity.setIsRlq(0);
                    pulsarEntity.setRetryLetterTopic(null);
                    streamService.logicDeleteDlqOrRlq(groupId, rlqNameOld, operator);
                } else if (!Objects.equals(rlqNameNew, pulsarEntity.getRetryLetterTopic())) {
                    pulsarEntity.setIsRlq(1);
                    String topic = BizConstant.PREFIX_RLQ + "_" + rlqNameNew;
                    topic = topic.toLowerCase(Locale.ROOT);
                    pulsarEntity.setRetryLetterTopic(topic);
                    streamService.insertDlqOrRlq(groupId, topic, operator);
                }
            }

            consumptionPulsarMapper.updateByConsumptionId(pulsarEntity);
        }

        consumptionMapper.updateByPrimaryKeySelective(entity);
        return true;
    }

    /**
     * According to groupId and topic, stitch the full path of Pulsar Topic
     * "persistent://" + tenant + "/" + namespace + "/" + topic;
     */
    private String getFullPulsarTopic(String groupId, String topic) {
        BusinessEntity businessEntity = businessMapper.selectByIdentifier(groupId);
        String tenant = clusterBean.getDefaultTenant();
        String namespace = businessEntity.getMqResourceObj();
        return "persistent://" + tenant + "/" + namespace + "/" + topic;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Boolean delete(Integer id, String operator) {
        ConsumptionEntity consumptionEntity = consumptionMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(consumptionEntity, "consumption not exist with id: " + id);
        consumptionMapper.deleteByPrimaryKey(id);

        consumptionPulsarMapper.deleteByConsumptionId(id);
        return true;
    }

    @Override
    public WorkflowResult startProcess(Integer id, String operation) {
        ConsumptionInfo consumptionInfo = this.get(id);
        Preconditions.checkTrue(ConsumptionStatus.ALLOW_START_WORKFLOW_STATUS.contains(
                        ConsumptionStatus.fromStatus(consumptionInfo.getStatus())),
                "current status not allow start workflow");

        ConsumptionEntity updateConsumptionEntity = new ConsumptionEntity();
        updateConsumptionEntity.setId(consumptionInfo.getId());
        updateConsumptionEntity.setModifyTime(new Date());
        updateConsumptionEntity.setStatus(ConsumptionStatus.WAITING_APPROVE.getStatus());
        int success = this.consumptionMapper.updateByPrimaryKeySelective(updateConsumptionEntity);
        Preconditions.checkTrue(success == 1, "update consumption failed");

        return workflowService.start(ProcessName.NEW_CONSUMPTION_WORKFLOW, operation,
                genNewConsumptionWorkflowForm(consumptionInfo));
    }

    @Override
    public void saveSortConsumption(BusinessInfo bizInfo, String topic, String consumerGroup) {
        String groupId = bizInfo.getInlongGroupId();
        ConsumptionEntity exists = consumptionMapper.selectConsumptionExists(groupId, topic, consumerGroup);
        if (exists != null) {
            log.warn("consumption with groupId={}, topic={}, consumer group={} already exists, skip to create",
                    groupId, topic, consumerGroup);
            return;
        }

        log.debug("begin to save consumption, groupId={}, topic={}, consumer group={}", groupId, topic, consumerGroup);
        String middlewareType = bizInfo.getMiddlewareType();
        ConsumptionEntity entity = new ConsumptionEntity();
        entity.setInlongGroupId(groupId);
        entity.setMiddlewareType(middlewareType);
        entity.setTopic(topic);
        entity.setConsumerGroupId(consumerGroup);
        entity.setInCharges(bizInfo.getInCharges());
        entity.setFilterEnabled(0);

        entity.setStatus(ConsumptionStatus.APPROVED.getStatus());
        entity.setIsDeleted(EntityStatus.UN_DELETED.getCode());
        entity.setCreator(bizInfo.getCreator());
        entity.setCreateTime(new Date());

        consumptionMapper.insert(entity);

        if (BizConstant.MIDDLEWARE_PULSAR.equals(middlewareType)) {
            ConsumptionPulsarEntity pulsarEntity = new ConsumptionPulsarEntity();
            pulsarEntity.setConsumptionId(entity.getId());
            pulsarEntity.setConsumerGroupId(consumerGroup);
            pulsarEntity.setInlongGroupId(groupId);
            pulsarEntity.setIsDeleted(EntityStatus.UN_DELETED.getCode());
            consumptionPulsarMapper.insert(pulsarEntity);
        }

        log.debug("success save consumption, groupId={}, topic={}, consumer group={}", groupId, topic, consumerGroup);
    }

    private NewConsumptionWorkflowForm genNewConsumptionWorkflowForm(ConsumptionInfo consumptionInfo) {
        NewConsumptionWorkflowForm form = new NewConsumptionWorkflowForm();
        Integer id = consumptionInfo.getId();
        if (BizConstant.MIDDLEWARE_PULSAR.equalsIgnoreCase(consumptionInfo.getMiddlewareType())) {
            ConsumptionPulsarEntity consumptionPulsarEntity = consumptionPulsarMapper.selectByConsumptionId(id);
            ConsumptionPulsarInfo pulsarInfo = CommonBeanUtils.copyProperties(consumptionPulsarEntity,
                    ConsumptionPulsarInfo::new);
            consumptionInfo.setMqExtInfo(pulsarInfo);
        }
        form.setConsumptionInfo(consumptionInfo);
        return form;
    }

    private ConsumptionEntity saveConsumption(ConsumptionInfo info, String operator, Date now) {
        ConsumptionEntity entity = CommonBeanUtils.copyProperties(info, ConsumptionEntity::new);
        entity.setStatus(ConsumptionStatus.WAITING_ASSIGN.getStatus());
        entity.setIsDeleted(0);
        entity.setCreator(operator);
        entity.setModifier(operator);
        entity.setCreateTime(now);
        entity.setModifyTime(now);

        if (info.getId() != null) {
            consumptionMapper.updateByPrimaryKey(entity);
        } else {
            consumptionMapper.insert(entity);
        }

        Preconditions.checkNotNull(entity.getId(), "save consumption failed");
        return entity;
    }

    /**
     * Fill in consumer information
     */
    private void fullConsumptionInfo(ConsumptionInfo info) {
        Preconditions.checkNotNull(info, "consumption info cannot be null");
        info.setConsumerGroupId(info.getConsumerGroupName());

        Preconditions.checkFalse(isConsumerGroupIdExists(info.getConsumerGroupId(), info.getId()),
                "consumer group " + info.getConsumerGroupId() + " already exist");

        if (info.getId() != null) {
            ConsumptionEntity consumptionEntity = consumptionMapper.selectByPrimaryKey(info.getId());
            Preconditions.checkNotNull(consumptionEntity, "consumption not exist with id: " + info.getId());

            ConsumptionStatus consumptionStatus = ConsumptionStatus.fromStatus(consumptionEntity.getStatus());
            Preconditions.checkTrue(ConsumptionStatus.ALLOW_SAVE_UPDATE_STATUS.contains(consumptionStatus),
                    "consumption not allow update when status is " + consumptionStatus.name());
        }

        // Determine whether the consumed topic belongs to this groupId or the data stream under it
        Preconditions.checkNotNull(info.getTopic(), "consumption topic cannot be empty");

        String groupId = info.getInlongGroupId();
        BusinessTopicVO topicVO = businessService.getTopic(groupId);
        Preconditions.checkNotNull(topicVO, "business not exist: " + groupId);

        // Tube’s topic is the business group level, one business group, one Tube topic
        if (BizConstant.MIDDLEWARE_TUBE.equalsIgnoreCase(topicVO.getMiddlewareType())) {
            String bizTopic = topicVO.getMqResourceObj();
            Preconditions.checkTrue(bizTopic == null || bizTopic.equals(info.getTopic()),
                    "topic [" + info.getTopic() + "] not belong to business " + groupId);
        } else if (BizConstant.MIDDLEWARE_PULSAR.equalsIgnoreCase(topicVO.getMiddlewareType())) {
            // Pulsar's topic is the data stream level.
            // There will be multiple data streams under one business, and there will be multiple topics
            List<DataStreamTopicVO> dsTopicList = topicVO.getDsTopicList();
            if (dsTopicList != null && dsTopicList.size() > 0) {
                Set<String> topicSet = new HashSet<>(Arrays.asList(info.getTopic().split(",")));
                dsTopicList.forEach(ds -> topicSet.remove(ds.getMqResourceObj()));
                Preconditions.checkEmpty(topicSet, "topic [" + topicSet + "] not belong to business " + groupId);
            }
        }
    }

}
