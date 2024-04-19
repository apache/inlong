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

package org.apache.inlong.manager.web.controller;

import org.apache.inlong.manager.pojo.audit.AuditBaseResponse;
import org.apache.inlong.manager.pojo.audit.AuditRequest;
import org.apache.inlong.manager.pojo.audit.AuditSourceRequest;
import org.apache.inlong.manager.pojo.audit.AuditSourceResponse;
import org.apache.inlong.manager.pojo.audit.AuditVO;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.UserInfo;
import org.apache.inlong.manager.service.core.AuditService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.when;

public class AuditControllerTest {

    @InjectMocks
    private AuditController auditController;

    @Mock
    private AuditService auditService;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testListByCondition() throws Exception {
        AuditRequest request = new AuditRequest();
        AuditVO auditVO = new AuditVO();
        auditVO.setAuditId("1");
        auditVO.setAuditName("Test Audit");
        List<AuditVO> mockData = Arrays.asList(auditVO);
        when(auditService.listByCondition(request)).thenReturn(mockData);

        Response<List<AuditVO>> response = auditController.listByCondition(request);

        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertEquals(Response.success(mockData).getData(), response.getData());
    }

    @Test
    public void testListAll() throws Exception {
        AuditRequest request = new AuditRequest();
        AuditVO auditVO = new AuditVO();
        auditVO.setAuditId("2");
        auditVO.setAuditName("Test Audit All");
        List<AuditVO> mockData = Arrays.asList(auditVO);
        when(auditService.listAll(request)).thenReturn(mockData);

        Response<List<AuditVO>> response = auditController.listAll(request);

        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertEquals(Response.success(mockData).getData(), response.getData());
    }

    @Test
    public void testRefreshCache() {
        when(auditService.refreshBaseItemCache()).thenReturn(true);

        Response<Boolean> response = auditController.refreshCache();

        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertEquals(Response.success(true).getData(), response.getData());
    }

    @Test
    public void testUpdateAuditSource() {
        AuditSourceRequest request = new AuditSourceRequest();
        when(auditService.updateAuditSource(request, "testUser")).thenReturn(1);
        UserInfo user = new UserInfo();
        user.setName("testUser");
        LoginUserUtils.setUserLoginInfo(user);
        Response<Integer> response = auditController.updateAuditSource(request);

        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertEquals(Response.success(1).getData(), response.getData());
    }

    @Test
    public void testGetAuditBases() {
        AuditBaseResponse auditBaseResponse = new AuditBaseResponse();
        auditBaseResponse.setId(1);
        auditBaseResponse.setName("Test Audit Base");
        List<AuditBaseResponse> mockData = Arrays.asList(auditBaseResponse);
        when(auditService.getAuditBases()).thenReturn(mockData);

        Response<List<AuditBaseResponse>> response = auditController.getAuditBases();

        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertEquals(Response.success(mockData).getData(), response.getData());
    }

    @Test
    public void testGetAuditSource() {
        AuditSourceResponse auditSourceResponse = new AuditSourceResponse();
        auditSourceResponse.setId(1);
        auditSourceResponse.setName("Test Audit Source");
        when(auditService.getAuditSource()).thenReturn(auditSourceResponse);

        Response<AuditSourceResponse> response = auditController.getAuditSource();

        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertEquals(Response.success(auditSourceResponse).getData(), response.getData());
    }
}