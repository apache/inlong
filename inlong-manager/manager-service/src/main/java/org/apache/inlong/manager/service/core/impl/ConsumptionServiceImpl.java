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
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.ConsumptionStatus;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.pojo.business.BusinessTopicVO;
import org.apache.inlong.manager.common.pojo.dataconsumption.ConsumptionInfo;
import org.apache.inlong.manager.common.pojo.dataconsumption.ConsumptionListVo;
import org.apache.inlong.manager.common.pojo.dataconsumption.ConsumptionQuery;
import org.apache.inlong.manager.common.pojo.dataconsumption.ConsumptionSummary;
import org.apache.inlong.manager.common.pojo.dataconsumption.ConsumptionUpdateInfo;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.ConsumptionEntity;
import org.apache.inlong.manager.dao.mapper.ConsumptionEntityMapper;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.core.ConsumptionService;
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
    private ConsumptionEntityMapper consumptionMapper;
    @Autowired
    private WorkflowService workflowService;
    @Autowired
    private BusinessService businessService;
    @Autowired
    private ClusterBean clusterBean;

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
        Page<ConsumptionEntity> pageResult = (Page<ConsumptionEntity>) this.consumptionMapper.listByQuery(query);
        PageInfo<ConsumptionListVo> pageInfo = pageResult
                .toPageInfo(entity -> CommonBeanUtils.copyProperties(entity, ConsumptionListVo::new));
        pageInfo.setTotal(pageResult.getTotal());
        return pageInfo;
    }

    @Override
    public ConsumptionInfo getInfo(Integer id) {
        Preconditions.checkNotNull(id, "Consumption id can't be null");
        ConsumptionEntity entity = consumptionMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, () -> "Consumption not exist with id:" + id);

        ConsumptionInfo consumptionInfo = CommonBeanUtils.copyProperties(entity, ConsumptionInfo::new);
        consumptionInfo.setMasterUrl(clusterBean.getTubeMaster());

        return consumptionInfo;
    }

    @Override
    public ConsumptionInfo getInfo(String consumerGroupId) {
        Preconditions.checkNotEmpty(consumerGroupId, "ConsumerGroupId can't be null");
        ConsumptionQuery consumptionQuery = new ConsumptionQuery();
        consumptionQuery.setConsumerGroupId(consumerGroupId);
        List<ConsumptionEntity> result = consumptionMapper.listByQuery(consumptionQuery);
        if (CollectionUtils.isEmpty(result)) {
            return null;
        }

        ConsumptionEntity entity = result.get(0);
        ConsumptionInfo info = CommonBeanUtils.copyProperties(entity, ConsumptionInfo::new);
        info.setMasterUrl(clusterBean.getTubeMaster());

        return info;
    }

    @Override
    public boolean isConsumerGroupIdExist(String groupId, Integer excludeSelfId) {
        ConsumptionQuery consumptionQuery = new ConsumptionQuery();
        consumptionQuery.setConsumerGroupId(groupId);
        List<ConsumptionEntity> result = consumptionMapper.listByQuery(consumptionQuery);
        if (excludeSelfId != null) {
            result = result.stream()
                    .filter(entity -> !excludeSelfId.equals(entity.getId()))
                    .collect(Collectors.toList());
        }
        return !CollectionUtils.isEmpty(result);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Integer save(ConsumptionInfo consumptionInfo, String operator) {
        checkConsumptionInfo(consumptionInfo);
        ConsumptionEntity consumptionEntity = saveConsumption(consumptionInfo, operator);

        log.debug("success to save consumption {}", consumptionInfo);
        return consumptionEntity.getId();
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Integer update(ConsumptionUpdateInfo info, String operator) {
        Preconditions.checkNotNull(info, "update info can't be null");
        Preconditions.checkNotNull(info.getId(), "consumption id can't be null");
        ConsumptionInfo consumptionInfo = this.getInfo(info.getId());
        Preconditions.checkNotNull(consumptionInfo, "consumption not exist with id " + info.getId());
        Preconditions.checkTrue(consumptionInfo.getInCharges().contains(operator),
                "operator is not the owner for this consumption");

        ConsumptionEntity entity = new ConsumptionEntity();
        entity.setId(info.getId());
        entity.setInCharges(info.getInCharges());
        entity.setModifier(operator);
        entity.setModifyTime(new Date());

        this.consumptionMapper.updateByPrimaryKeySelective(entity);

        log.debug("success to update consumption {}", entity);
        return entity.getId();
    }

    @Override
    public void delete(Integer id, String operator) {
        ConsumptionEntity consumptionEntity = consumptionMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(consumptionEntity, () -> "consumption not exist with id:" + id);
        int success = consumptionMapper.deleteByPrimaryKey(id);
        Preconditions.checkTrue(success > 0, "delete failed");
    }

    @Override
    public WorkflowResult startProcess(Integer id, String operation) {
        ConsumptionInfo consumptionInfo = this.getInfo(id);
        Preconditions.checkTrue(ConsumptionStatus.ALLOW_START_WORKFLOW_STATUS
                        .contains(ConsumptionStatus.fromStatus(consumptionInfo.getStatus())),
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

    private NewConsumptionWorkflowForm genNewConsumptionWorkflowForm(ConsumptionInfo consumptionInfo) {
        NewConsumptionWorkflowForm form = new NewConsumptionWorkflowForm();
        form.setConsumptionInfo(consumptionInfo);
        return form;
    }

    private ConsumptionEntity saveConsumption(ConsumptionInfo consumptionInfo, String operator) {
        Date now = new Date();
        consumptionInfo.setCreateTime(now);
        consumptionInfo.setModifyTime(now);
        consumptionInfo.setModifier(operator);
        consumptionInfo.setCreator(operator);

        ConsumptionEntity entity = CommonBeanUtils.copyProperties(consumptionInfo, ConsumptionEntity::new);
        entity.setStatus(ConsumptionStatus.WAITING_ASSIGN.getStatus());
        entity.setIsDeleted(EntityStatus.UN_DELETED.getCode());

        int success;
        if (consumptionInfo.getId() != null) {
            success = this.consumptionMapper.updateByPrimaryKey(entity);
        } else {
            success = this.consumptionMapper.insert(entity);
        }
        Preconditions.checkTrue(success > 0, "save failed");
        Preconditions.checkNotNull(entity.getId(), "save failed");
        return entity;
    }

    /**
     * Check whether the consumption information is valid
     */
    private void checkConsumptionInfo(ConsumptionInfo info) {
        Preconditions.checkNotNull(info, "Consumption info can't be null");
        Preconditions.checkNotNull(info.getConsumerGroupName(), "Consumer Group Name can't be null");

        // Undeleted consumer group id must be unique
        String groupId = info.getConsumerGroupName().toLowerCase(Locale.ROOT);
        info.setConsumerGroupId(groupId);
        Preconditions.checkTrue(!isConsumerGroupIdExist(groupId, info.getId()),
                "Consumer Group ID " + groupId + " already exist");

        if (info.getId() != null) {
            ConsumptionEntity entity = consumptionMapper.selectByPrimaryKey(info.getId());
            Preconditions.checkNotNull(entity, "Consumption not exist with id:" + info.getId());

            ConsumptionStatus consumptionStatus = ConsumptionStatus.fromStatus(entity.getStatus());
            Preconditions.checkTrue(ConsumptionStatus.ALLOW_SAVE_UPDATE_STATUS.contains(consumptionStatus),
                    "Consumption not allowed to update when its status is" + consumptionStatus.name());
        }

        BusinessTopicVO topicVO = businessService.getTopic(info.getInlongGroupId());
        Preconditions.checkNotNull(topicVO, "Business not exist :" + info.getInlongGroupId());
        Preconditions.checkTrue(topicVO.getTopicName() != null
                        && topicVO.getTopicName().equals(info.getTopic()),
                "Topic [" + info.getTopic() + "] not belong to business " + info.getInlongGroupId());

    }

}
