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

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.dao.entity.AuditIdEntity;
import org.apache.inlong.manager.dao.mapper.AuditIdEntityMapper;
import org.apache.inlong.manager.service.core.AuditIdService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Audit id service layer implementation
 */
@Service
public class AuditIdServiceImpl implements AuditIdService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditIdServiceImpl.class);

    // key : audit id type, value : audit id info
    private final Map<String, AuditIdEntity> auditIdSentMap = new ConcurrentHashMap<>();

    private final Map<String, AuditIdEntity> auditIdReceivedMap = new ConcurrentHashMap<>();

    @Autowired
    private AuditIdEntityMapper auditIdMapper;

    @PostConstruct
    public void initialize() {
        LOGGER.info("init auditIdEntityMap for " + AuditServiceImpl.class.getSimpleName());
        try {
            refreshCache();
        } catch (Throwable t) {
            LOGGER.error("initialize auditIdEntityMap error", t);
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void refreshCache() {
        LOGGER.debug("start to reload audit id info.");
        try {
            List<AuditIdEntity> auditIdEntityList = auditIdMapper.selectAll();
            for (AuditIdEntity auditIdEntity : auditIdEntityList) {
                String type = auditIdEntity.getType();
                if (auditIdEntity.getIsSent() == 1) {
                    auditIdSentMap.put(type, auditIdEntity);
                } else {
                    auditIdReceivedMap.put(type, auditIdEntity);
                }
            }
        } catch (Throwable t) {
            LOGGER.error("fail to reload audit id info", t);
        }
        LOGGER.debug("end to reload audit id info");
    }

    @Override
    public String getAuditId(String type, boolean isSent) {
        AuditIdEntity auditIdEntity = isSent ? auditIdSentMap.get(type) : auditIdReceivedMap.get(type);
        if (auditIdEntity == null) {
            throw new BusinessException(ErrorCodeEnum.AUDIT_ID_TYPE_NOT_SUPPORTED,
                    String.format(ErrorCodeEnum.AUDIT_ID_TYPE_NOT_SUPPORTED.getMessage(), type));
        }
        return auditIdEntity.getAuditId();
    }

}
