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

import org.apache.inlong.manager.dao.mapper.AuditQuerySourceConfigEntityMapper;
import org.apache.inlong.manager.pojo.audit.AuditInfo;
import org.apache.inlong.manager.pojo.audit.AuditRequest;
import org.apache.inlong.manager.pojo.audit.AuditSourceRequest;
import org.apache.inlong.manager.pojo.audit.AuditVO;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.AuditService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Audit service test for {@link AuditService}
 */
class AuditServiceTest extends ServiceBaseTest {

    @Autowired
    private AuditService auditService;

    @Test
    void testQueryFromMySQL() throws IOException {
        AuditRequest request = new AuditRequest();
        request.setAuditIds(Arrays.asList("3", "4"));
        request.setInlongGroupId("g1");
        request.setInlongStreamId("s1");
        request.setStartDate("2022-01-01");
        request.setEndDate("2022-01-01");
        List<AuditVO> result = new ArrayList<>();
        AuditVO auditVO = new AuditVO();
        auditVO.setAuditId("3");
        auditVO.setAuditSet(Arrays.asList(new AuditInfo("2022-01-01 00:00:00", 123L, 12L),
                new AuditInfo("2022-01-01 00:01:00", 124L, 12L)));
        result.add(auditVO);
        Assertions.assertNotNull(result);
        // close real test for testQueryFromMySQL due to date_format function not support in h2
        // Assertions.assertNotNull(auditService.listByCondition(request));
    }

    /**
     * Temporarily close testing for testQueryFromElasticsearch due to lack of elasticsearch dev environment
     * You can open it if exists elasticsearch dev environment
     *
     * @throws IOException The exception may throws
     */
    // @Test
    public void testQueryFromElasticsearch() throws Exception {
        AuditRequest request = new AuditRequest();
        request.setAuditIds(Arrays.asList("3", "4"));
        request.setInlongGroupId("g1");
        request.setInlongStreamId("s1");
        request.setStartDate("2022-01-01");
        request.setEndDate("2022-01-01");
        Assertions.assertNotNull(auditService.listByCondition(request));
    }

    @Autowired
    AuditQuerySourceConfigEntityMapper querySourceConfigEntityMapper;
    @Test
    void testUpdateAuditQuerySource() {
        AuditSourceRequest request = new AuditSourceRequest(null, "source1",
                "host1",
                "default", "123456", 1);
        auditService.updateAuditQuerySource(request);
        AuditSourceRequest request2 = new AuditSourceRequest("host1", "source2",
                "host2",
                "default", "123456", 1);
        auditService.updateAuditQuerySource(request2);
        Assertions.assertEquals(auditService.queryCurrentSource().getHosts(),
                request2.getHosts());
    }
}
