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

package org.apache.inlong.tool.service;

import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.entity.AuditMetric;
import org.apache.inlong.audit.tool.mapper.AuditMapper;
import org.apache.inlong.audit.tool.service.AuditMetricService;
import org.apache.inlong.audit.tool.util.AuditSQLUtil;

import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class AuditMetricServiceTest {

    @Test
    public void getStorageAuditMetricsReturnsEmptyListWhenNoDataFound() {
        AuditMetricService auditMetricService = new AuditMetricService();

        try (MockedStatic<AuditSQLUtil> sqlUtilMockedStatic = Mockito.mockStatic(AuditSQLUtil.class)) {
            SqlSession sqlSessionMock = Mockito.mock(SqlSession.class);
            AuditMapper auditMapperMock = Mockito.mock(AuditMapper.class);

            sqlUtilMockedStatic.when(AuditSQLUtil::getSqlSession).thenReturn(sqlSessionMock);
            Mockito.when(sqlSessionMock.getMapper(AuditMapper.class)).thenReturn(auditMapperMock);
            Mockito.when(auditMapperMock.getAuditMetrics(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                    .thenReturn(Collections.emptyList());

            List<AuditMetric> result = auditMetricService.getStorageAuditMetrics("nonexistentId", "2023-01-01 00:00:00",
                    "2023-01-01 01:00:00");
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void getStorageAuditMetricsHandlesInvalidTimestampsGracefully() {
        // Query service initialization
        AppConfig appConfig = new AppConfig();
        AuditSQLUtil.initialize(appConfig.getProperties());
        AuditMetricService auditMetricService = new AuditMetricService();

        String invalidStartLogTs = "invalid-timestamp";
        String invalidEndLogTs = "invalid-timestamp";

        List<AuditMetric> result = auditMetricService.getStorageAuditMetrics("5", invalidStartLogTs, invalidEndLogTs);

        assertTrue(result.isEmpty());
    }

}
