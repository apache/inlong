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

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.ConsumptionStatus;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongConsumeEntity;
import org.apache.inlong.manager.dao.mapper.InlongConsumeEntityMapper;
import org.apache.inlong.manager.pojo.common.CountInfo;
import org.apache.inlong.manager.pojo.consumption.ConsumptionListVo;
import org.apache.inlong.manager.pojo.consumption.ConsumptionQuery;
import org.apache.inlong.manager.pojo.consumption.ConsumptionSummary;
import org.apache.inlong.manager.pojo.consumption.InlongConsumeInfo;
import org.apache.inlong.manager.pojo.consumption.InlongConsumeRequest;
import org.apache.inlong.manager.pojo.user.UserRoleCode;
import org.apache.inlong.manager.service.user.LoginUserUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


/**
 * Inlong consume service layer implementation
 */
@Slf4j
@Service
public class InlongConsumeServiceImpl implements InlongConsumeService {

    @Autowired
    private InlongConsumeEntityMapper consumeEntityMapper;

    @Autowired
    private InlongConsumeOperatorFactory consumeOperatorFactory;

    @Override
    public Integer save(InlongConsumeRequest consumeRequest, String operator) {
        log.debug("begin to save consumption info={}", consumeRequest);
        Preconditions.checkNotNull(consumeRequest, "consumption info cannot be null");
        Preconditions.checkNotNull(consumeRequest.getTopic(), "consumption topic cannot be empty");
        if (isConsumerGroupExists(consumeRequest.getConsumerGroup(), consumeRequest.getId())) {
            throw new BusinessException(String.format("consumer group %s already exist",
                    consumeRequest.getConsumerGroup()));
        }

        if (consumeRequest.getId() != null) {
            InlongConsumeEntity consumeEntity = consumeEntityMapper.selectByPrimaryKey(consumeRequest.getId());
            Preconditions.checkNotNull(consumeEntity, "consumption not exist with id: " + consumeRequest.getId());
            ConsumptionStatus consumptionStatus = ConsumptionStatus.fromStatus(consumeEntity.getStatus());
            Preconditions.checkTrue(ConsumptionStatus.ALLOW_SAVE_UPDATE_STATUS.contains(consumptionStatus),
                    "consumption not allow update when status is " + consumptionStatus.name());
        }

        InlongConsumeOperator instance = consumeOperatorFactory.getInstance(consumeRequest.getMqType());
        instance.setTopicInfo(consumeRequest);
        instance.saveOpt(consumeRequest, operator);
        return consumeRequest.getId();
    }

    @Override
    public boolean isConsumerGroupExists(String consumerGroup, Integer excludeSelfId) {
        ConsumptionQuery consumptionQuery = new ConsumptionQuery();
        consumptionQuery.setConsumerGroup(consumerGroup);
        consumptionQuery.setIsAdminRole(true);
        List<InlongConsumeEntity> result = consumeEntityMapper.listByQuery(consumptionQuery);
        if (excludeSelfId != null) {
            result = result.stream().filter(consumer -> !excludeSelfId.equals(consumer.getId()))
                    .collect(Collectors.toList());
        }
        return CollectionUtils.isNotEmpty(result);
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Boolean update(InlongConsumeRequest consumeRequest, String operator) {
        Preconditions.checkNotNull(consumeRequest, "consumption info cannot be null");
        Integer consumptionId = consumeRequest.getId();
        InlongConsumeEntity exists = consumeEntityMapper.selectByPrimaryKey(consumptionId);
        Preconditions.checkNotNull(exists, "consumption not exist with id " + consumptionId);
        Preconditions.checkTrue(exists.getInCharges().contains(operator),
                "operator" + operator + " has no privilege for the consumption");
        String errMsg = String.format("consumption information has already updated, id=%s, curVersion=%s",
                exists.getId(), consumeRequest.getVersion());
        if (!Objects.equals(exists.getVersion(), consumeRequest.getVersion())) {
            log.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }

        // Modify Pulsar consumption info
        InlongConsumeOperator instance = consumeOperatorFactory.getInstance(consumeRequest.getMqType());
        instance.updateOpt(consumeRequest, exists, operator);
        return true;
    }

    @Override
    public InlongConsumeInfo get(Integer id) {
        Preconditions.checkNotNull(id, "consumption id cannot be null");
        InlongConsumeEntity entity = consumeEntityMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "consumption not exist with id:" + id);

        InlongConsumeOperator instance = consumeOperatorFactory.getInstance(entity.getMqType());
        return instance.getFromEntity(entity);
    }

    @Override
    public Boolean delete(Integer id, String operator) {
        InlongConsumeEntity consumeEntity = consumeEntityMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(consumeEntity, "consumption not exist with id: " + id);
        consumeEntityMapper.deleteByPrimaryKey(id);
        return true;
    }

    @Override
    public PageInfo<ConsumptionListVo> list(ConsumptionQuery query) {
        PageHelper.startPage(query.getPageNum(), query.getPageSize());
        query.setIsAdminRole(LoginUserUtils.getLoginUser().getRoles().contains(UserRoleCode.ADMIN));
        Page<InlongConsumeEntity> pageResult = (Page<InlongConsumeEntity>) consumeEntityMapper.listByQuery(query);
        PageInfo<ConsumptionListVo> pageInfo = pageResult
                .toPageInfo(entity -> CommonBeanUtils.copyProperties(entity, ConsumptionListVo::new));
        pageInfo.setTotal(pageResult.getTotal());
        return pageInfo;
    }

    @Override
    public ConsumptionSummary getSummary(ConsumptionQuery query) {
        Map<String, Integer> countMap = consumeEntityMapper.countByQuery(query)
                .stream()
                .collect(Collectors.toMap(CountInfo::getKey, CountInfo::getValue));

        return ConsumptionSummary.builder()
                .totalCount(countMap.values().stream().mapToInt(c -> c).sum())
                .waitingAssignCount(countMap.getOrDefault(ConsumptionStatus.WAIT_ASSIGN.getStatus() + "", 0))
                .waitingApproveCount(countMap.getOrDefault(ConsumptionStatus.WAIT_APPROVE.getStatus() + "", 0))
                .rejectedCount(countMap.getOrDefault(ConsumptionStatus.REJECTED.getStatus() + "", 0)).build();
    }
}
