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
import org.apache.inlong.audit.tool.service.AuditMetricService;
import org.apache.inlong.audit.tool.task.AuditCheckTask;
import org.apache.inlong.audit.tool.util.AuditSQLUtil;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class AuditMetricServiceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AuditMetricServiceTest.class);

    @Test
    public void testGetDataProxyAuditMetrics() {
        // Query service initialization
        AppConfig appConfig = new AppConfig();
        AuditSQLUtil.initialize(appConfig.getProperties());
        AuditMetricService auditMetricService = new AuditMetricService();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String endLogTs = LocalDateTime.now().minusMinutes(1).format(formatter);
        String startLogTs = LocalDateTime.now().minusMinutes(5).format(formatter);

        // Search for relevant data
        List<AuditMetric> dataproxyAuditMetrics =
                auditMetricService.getStorageAuditMetrics("5", startLogTs, endLogTs);

        for (AuditMetric auditMetric : dataproxyAuditMetrics) {
            LOGGER.error("{} {} {}", auditMetric.getInlongGroupId(), auditMetric.getInlongStreamId(), auditMetric.getCount());
        }
    }
}
