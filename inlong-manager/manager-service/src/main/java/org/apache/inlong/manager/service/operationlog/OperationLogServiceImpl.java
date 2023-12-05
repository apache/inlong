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

package org.apache.inlong.manager.service.operationlog;

import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.OperationLogEntity;
import org.apache.inlong.manager.dao.mapper.OperationLogEntityMapper;
import org.apache.inlong.manager.pojo.common.OrderFieldEnum;
import org.apache.inlong.manager.pojo.common.OrderTypeEnum;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.operationLog.OperationLogRequest;
import org.apache.inlong.manager.pojo.operationLog.OperationLogResponse;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.inlong.manager.pojo.common.PageRequest.MAX_PAGE_SIZE;

/**
 * Operation log operation
 */
@Service
public class OperationLogServiceImpl implements OperationLogService {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperationLogServiceImpl.class);

    @Autowired
    private OperationLogEntityMapper operationLogMapper;

    @Override
    public PageResult<OperationLogResponse> listByCondition(OperationLogRequest request) {
        if (request.getPageSize() > MAX_PAGE_SIZE) {
            LOGGER.warn("list operation logs, change page size from {} to {}", request.getPageSize(), MAX_PAGE_SIZE);
            request.setPageSize(MAX_PAGE_SIZE);
        }
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        OrderFieldEnum.checkOrderField(request);
        OrderTypeEnum.checkOrderType(request);
        Page<OperationLogEntity> entityPage = (Page<OperationLogEntity>) operationLogMapper.selectByCondition(request);
        PageResult<OperationLogResponse> pageResult = PageResult.fromPage(entityPage)
                .map(entity -> CommonBeanUtils.copyProperties(entity, OperationLogResponse::new));
        return pageResult;
    }
}
