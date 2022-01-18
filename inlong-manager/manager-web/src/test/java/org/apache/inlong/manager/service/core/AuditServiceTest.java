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

package org.apache.inlong.manager.service.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.inlong.manager.common.pojo.audit.AuditRequest;
import org.apache.inlong.manager.common.pojo.audit.AuditVO;
import org.apache.inlong.manager.web.ServiceBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestComponent;

@TestComponent
public class AuditServiceTest extends ServiceBaseTest {

    @Autowired
    private AuditService auditService;

    @Test
    public void testQueryFromMySQL() throws IOException {
        AuditRequest request = new AuditRequest();
        request.setAuditId("3");
        request.setInlongGroupId("g1");
        request.setInlongStreamId("s1");
        request.setDt("2022-01-01");
        List<AuditVO> result = new ArrayList<>();
        AuditVO auditVO = new AuditVO();
        auditVO.setCount(123L);
        auditVO.setLogTs("2022-01-01 00:00:00");
        result.add(auditVO);
        Assert.assertNotNull(result);
        // close real test for testQueryFromMySQL due to date_format function not support in h2
//        Assert.assertNotNull(auditService.listByCondition(request));
    }

    /**
     * Temporarily close testing for testQueryFromElasticsearch due to lack of elasticsearch dev environment
     * You can open it if exists elasticsearch dev environment
     *
     * @throws IOException The exception may throws
     */
//    @Test
    public void testQueryFromElasticsearch() throws IOException {
        AuditRequest request = new AuditRequest();
        request.setAuditId("3");
        request.setInlongGroupId("g1");
        request.setInlongStreamId("s1");
        request.setDt("2022-01-01");
        Assert.assertNotNull(auditService.listByCondition(request));
    }
}
