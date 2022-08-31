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

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ConsumeStatus;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongConsumeEntity;
import org.apache.inlong.manager.dao.mapper.InlongConsumeEntityMapper;
import org.apache.inlong.manager.pojo.consume.InlongConsumeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Default operator of inlong consume.
 */
public abstract class AbstractConsumeOperator implements InlongConsumeOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConsumeOperator.class);

    @Autowired
    private InlongConsumeEntityMapper consumeEntityMapper;

    protected abstract void setExtParam(InlongConsumeRequest consumeRequest, InlongConsumeEntity entity);

    protected abstract void updateExtParam(InlongConsumeRequest consumeRequest, InlongConsumeEntity exists,
            String operator);

    public void saveOpt(InlongConsumeRequest consumeRequest, String operator) {
        InlongConsumeEntity entity = CommonBeanUtils.copyProperties(consumeRequest, InlongConsumeEntity::new);
        entity.setStatus(ConsumeStatus.WAIT_ASSIGN.getCode());
        entity.setCreator(operator);
        entity.setModifier(operator);

        setExtParam(consumeRequest, entity);

        if (consumeRequest.getId() != null) {
            int rowCount = consumeEntityMapper.updateByPrimaryKey(entity);
            if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
                LOGGER.error("inlong consume has already updated, id={}, curVersion={}",
                        entity.getId(), entity.getVersion());
                throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
            }
        } else {
            consumeEntityMapper.insert(entity);
        }

        Preconditions.checkNotNull(entity.getId(), "inlong consume save failed");
    }

    public void updateOpt(InlongConsumeRequest consumeRequest, InlongConsumeEntity exists, String operator) {
        updateExtParam(consumeRequest, exists, operator);

        int rowCount = consumeEntityMapper.updateByIdSelective(exists);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            // LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
    }
}
