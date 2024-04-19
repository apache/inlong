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

import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.heartbeat.ComponentHeartbeatResponse;
import org.apache.inlong.manager.pojo.heartbeat.GroupHeartbeatResponse;
import org.apache.inlong.manager.pojo.heartbeat.HeartbeatPageRequest;
import org.apache.inlong.manager.pojo.heartbeat.HeartbeatQueryRequest;
import org.apache.inlong.manager.pojo.heartbeat.StreamHeartbeatResponse;
import org.apache.inlong.manager.service.core.HeartbeatService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.when;

public class HeartbeatControllerTest {

    @Mock
    private HeartbeatService heartbeatService;

    @InjectMocks
    private HeartbeatController heartbeatController;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetComponentHeartbeat() {
        // Mock the request and response
        HeartbeatQueryRequest request = new HeartbeatQueryRequest();
        ComponentHeartbeatResponse data = new ComponentHeartbeatResponse();
        data.setComponent("Test component");
        data.setInstance("Test instance");
        data.setStatusHeartbeat("Test statusHeartbeat");
        data.setMetricHeartbeat("Test metricHeartbeat");

        // Mock the service method
        when(heartbeatService.getComponentHeartbeat(request)).thenReturn(data);

        // Call the controller method
        Response<ComponentHeartbeatResponse> response = heartbeatController.getComponentHeartbeat(request);

        // Verify the result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertEquals(Response.success(data).getData(), response.getData());
    }

    @Test
    public void testGetGroupHeartbeat() {
        // Mock the request and response
        HeartbeatQueryRequest request = new HeartbeatQueryRequest();
        GroupHeartbeatResponse data = new GroupHeartbeatResponse();
        data.setId(1);
        data.setComponent("Test component");
        data.setInstance("Test instance");
        data.setStatusHeartbeat("Test statusHeartbeat");
        data.setMetricHeartbeat("Test metricHeartbeat");

        // Mock the service method
        when(heartbeatService.getGroupHeartbeat(request)).thenReturn(data);

        // Call the controller method
        Response<GroupHeartbeatResponse> response = heartbeatController.getGroupHeartbeat(request);

        // Verify the result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertEquals(Response.success(data).getData(), response.getData());
    }

    @Test
    public void testGetStreamHeartbeat() {
        // Mock the request and response
        HeartbeatQueryRequest request = new HeartbeatQueryRequest();
        StreamHeartbeatResponse data = new StreamHeartbeatResponse();
        data.setId(1);
        data.setComponent("Test component");
        data.setInstance("Test instance");
        data.setStatusHeartbeat("Test statusHeartbeat");
        data.setMetricHeartbeat("Test metricHeartbeat");

        // Mock the service method
        when(heartbeatService.getStreamHeartbeat(request)).thenReturn(data);

        // Call the controller method
        Response<StreamHeartbeatResponse> response = heartbeatController.getStreamHeartbeat(request);

        // Verify the result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertEquals(Response.success(data).getData(), response.getData());

    }

    @Test
    public void testListComponentHeartbeat() {
        // Mock the request and response
        HeartbeatPageRequest request = new HeartbeatPageRequest();
        PageResult<ComponentHeartbeatResponse> dataPage = new PageResult<>();
        ComponentHeartbeatResponse data = new ComponentHeartbeatResponse();
        data.setComponent("Test component");
        data.setInstance("Test instance");
        data.setStatusHeartbeat("Test statusHeartbeat");
        data.setMetricHeartbeat("Test metricHeartbeat");
        List<ComponentHeartbeatResponse> dataList = Collections.singletonList(data);
        dataPage.setList(dataList);
        dataPage.setPageSize(20);
        dataPage.setPageNum(1);

        // Mock the service method
        when(heartbeatService.listComponentHeartbeat(request)).thenReturn(dataPage);

        // Call the controller method
        Response<PageResult<ComponentHeartbeatResponse>> response = heartbeatController.listComponentHeartbeat(request);

        // Verify the result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertEquals(Response.success(data).getData(),
                response.getData().getList().get(0));
    }

    @Test
    public void testListGroupHeartbeat() {
        // Mock the request and response
        HeartbeatPageRequest request = new HeartbeatPageRequest();
        PageResult<GroupHeartbeatResponse> dataPage = new PageResult<>();
        GroupHeartbeatResponse data = new GroupHeartbeatResponse();
        data.setComponent("Test component");
        data.setInstance("Test instance");
        data.setStatusHeartbeat("Test statusHeartbeat");
        data.setMetricHeartbeat("Test metricHeartbeat");
        List<GroupHeartbeatResponse> dataList = Collections.singletonList(data);
        dataPage.setList(dataList);
        dataPage.setPageSize(20);
        dataPage.setPageNum(1);

        // Mock the service method
        when(heartbeatService.listGroupHeartbeat(request)).thenReturn(dataPage);

        // Call the controller method
        Response<PageResult<GroupHeartbeatResponse>> response = heartbeatController.listGroupHeartbeat(request);

        // Verify the result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertEquals(Response.success(data).getData(),
                response.getData().getList().get(0));
    }

    @Test
    public void testListStreamHeartbeat() {
        // Mock the request and response
        HeartbeatPageRequest request = new HeartbeatPageRequest();
        PageResult<StreamHeartbeatResponse> dataPage = new PageResult<>();
        StreamHeartbeatResponse data = new StreamHeartbeatResponse();
        data.setComponent("Test component");
        data.setInstance("Test instance");
        data.setStatusHeartbeat("Test statusHeartbeat");
        data.setMetricHeartbeat("Test metricHeartbeat");
        List<StreamHeartbeatResponse> dataList = Collections.singletonList(data);
        dataPage.setList(dataList);
        dataPage.setPageSize(20);
        dataPage.setPageNum(1);

        // Mock the service method
        when(heartbeatService.listStreamHeartbeat(request)).thenReturn(dataPage);

        // Call the controller method
        Response<PageResult<StreamHeartbeatResponse>> response = heartbeatController.listStreamHeartbeat(request);

        // Verify the result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertEquals(Response.success(data).getData(),
                response.getData().getList().get(0));
    }
}