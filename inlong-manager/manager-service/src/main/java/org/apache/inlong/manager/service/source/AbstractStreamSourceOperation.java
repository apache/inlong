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

package org.apache.inlong.manager.service.source;

import java.util.Date;
import javax.validation.constraints.NotNull;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.SourceState;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class AbstractStreamSourceOperation implements StreamSourceOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStreamSourceOperation.class);
    @Autowired
    protected StreamSourceEntityMapper sourceMapper;

    /**
     * Setting the parameters of the latest entity.
     *
     * @param request source request
     * @param targetEntity entity object which will set the new parameters.
     */
    protected abstract void setTargetEntity(SourceRequest request, StreamSourceEntity targetEntity);

    /**
     * Getting the source type.
     *
     * @return source type string.
     */
    protected abstract String getSourceType();

    /**
     * Creating source response object.
     *
     * @return response object.
     */
    protected abstract SourceResponse getResponse();

    @Override
    public SourceResponse getById(@NotNull Integer id) {
        StreamSourceEntity entity = sourceMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SOURCE_INFO_NOT_FOUND.getMessage());
        String existType = entity.getSourceType();
        Preconditions.checkTrue(getSourceType().equals(existType),
                String.format(Constant.SOURCE_TYPE_NOT_SAME, getSourceType(), existType));
        return this.getFromEntity(entity, this::getResponse);
    }

    @Override
    public void updateOpt(SourceRequest request, String operator) {
        StreamSourceEntity entity = sourceMapper.selectByPrimaryKey(request.getId());
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SOURCE_INFO_NOT_FOUND.getMessage());
        final SourceState curState = SourceState.forCode(entity.getStatus());
        // Setting updated parameters of stream source entity.
        setTargetEntity(request, entity);
        entity.setModifier(operator);
        entity.setModifyTime(new Date());
        sourceMapper.updateByPrimaryKeySelective(entity);
        LOGGER.info("success to update source of type={}", request.getSourceType());
    }

    @Override
    public Integer saveOpt(SourceRequest request, String operator) {
        StreamSourceEntity entity = CommonBeanUtils.copyProperties(request, StreamSourceEntity::new);
        entity.setStatus(SourceState.SOURCE_ADD.getCode());
        entity.setIsDeleted(EntityStatus.UN_DELETED.getCode());
        entity.setCreator(operator);
        entity.setModifier(operator);
        Date now = new Date();
        entity.setCreateTime(now);
        entity.setModifyTime(now);
        // get the ext params
        setTargetEntity(request, entity);
        sourceMapper.insert(entity);
        return entity.getId();
    }

    @Override
    public void stopOpt(SourceRequest request, String operator) {
        StreamSourceEntity snapshot = sourceMapper.selectByPrimaryKey(request.getId());
        SourceState curState = SourceState.forCode(snapshot.getStatus());
        SourceState nextState = SourceState.SOURCE_FROZEN;
        if (!SourceState.isAllowedTransition(curState, nextState)) {
            throw new RuntimeException(String.format("Source=%s is not allowed to stop", snapshot));
        }
        StreamSourceEntity curEntity = CommonBeanUtils.copyProperties(request, StreamSourceEntity::new);
        curEntity.setModifyTime(new Date());
        curEntity.setPreviousStatus(curState.getCode());
        curEntity.setStatus(nextState.getCode());
        sourceMapper.updateByPrimaryKeySelective(curEntity);
    }

    @Override
    public void restartOpt(SourceRequest request, String operator) {
        StreamSourceEntity snapshot = sourceMapper.selectByPrimaryKey(request.getId());
        SourceState curState = SourceState.forCode(snapshot.getStatus());
        SourceState nextState = SourceState.SOURCE_ACTIVE;
        if (!SourceState.isAllowedTransition(curState, nextState)) {
            throw new RuntimeException(String.format("Source=%s is not allowed to restart", snapshot));
        }
        StreamSourceEntity curEntity = CommonBeanUtils.copyProperties(request, StreamSourceEntity::new);
        curEntity.setModifyTime(new Date());
        curEntity.setPreviousStatus(curState.getCode());
        curEntity.setStatus(nextState.getCode());

        sourceMapper.updateByPrimaryKeySelective(curEntity);
    }

    @Override
    public void deleteOpt(SourceRequest request, String operator) {
        StreamSourceEntity snapshot = sourceMapper.selectByPrimaryKey(request.getId());
        SourceState curState = SourceState.forCode(snapshot.getStatus());
        SourceState nextState = SourceState.SOURCE_DEL;
        if (!SourceState.isAllowedTransition(curState, nextState)) {
            throw new RuntimeException(String.format("Source=%s is not allowed to delete", snapshot));
        }
        StreamSourceEntity curEntity = CommonBeanUtils.copyProperties(request, StreamSourceEntity::new);
        curEntity.setModifyTime(new Date());
        curEntity.setPreviousStatus(curState.getCode());
        curEntity.setStatus(nextState.getCode());
        sourceMapper.updateByPrimaryKeySelective(curEntity);
    }
}
