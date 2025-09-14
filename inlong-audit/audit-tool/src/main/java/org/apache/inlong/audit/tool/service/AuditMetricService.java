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

package org.apache.inlong.audit.tool.service;

import org.apache.inlong.audit.tool.entity.AuditMetric;
import org.apache.inlong.audit.tool.mapper.AuditMapper;
import org.apache.inlong.audit.tool.util.AuditSQLUtil;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class AuditMetricService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditMetricService.class);

    public List<AuditMetric> getStorageAuditMetrics(String auditId, String startLogTs, String endLogTs) {
        try (SqlSession sqlSession = AuditSQLUtil.getSqlSession()) {
            AuditMapper auditMapper = sqlSession.getMapper(AuditMapper.class);
            return auditMapper.getAuditMetrics(startLogTs, endLogTs, auditId);
        } catch (Exception e) {
            LOGGER.error("Exception occurred during database query: ", e);
            return Collections.emptyList();
        }
    }

}