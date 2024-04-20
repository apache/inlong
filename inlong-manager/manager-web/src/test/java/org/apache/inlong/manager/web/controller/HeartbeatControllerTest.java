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

import org.apache.inlong.common.heartbeat.GroupHeartbeat;
import org.apache.inlong.common.heartbeat.StreamHeartbeat;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.heartbeat.ComponentHeartbeatResponse;
import org.apache.inlong.manager.pojo.heartbeat.GroupHeartbeatResponse;
import org.apache.inlong.manager.pojo.heartbeat.HeartbeatQueryRequest;
import org.apache.inlong.manager.pojo.heartbeat.HeartbeatReportRequest;
import org.apache.inlong.manager.pojo.heartbeat.StreamHeartbeatResponse;
import org.apache.inlong.manager.service.core.HeartbeatService;
import org.apache.inlong.manager.web.WebBaseTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class HeartbeatControllerTest extends WebBaseTest {

    @Autowired
    private HeartbeatService heartbeatService;

    @InjectMocks
    private HeartbeatController heartbeatController;

    private static final String TEST_PWD = "test_#$%%Y@UI$123";
    private static final String INSTANCE_IP = "127.0.0.1";
    private static final String COMPONENT_TYPE = "DATAPROXY";
    private static final String INLONG_GROUP_ID_1 = "test_group_heartbeat_1";
    private static final String INLONG_GROUP_ID_2 = "test_group_heartbeat_2";
    private static final String INLONG_STREAM_ID_1 = "test_stream_heartbeat_1";
    private static final String INLONG_STREAM_ID_2 = "test_stream_heartbeat_2";
    private static final String STATUS = "enabled";
    private static final String CLUSTER_NAME = "test_cluster";
    private static final String CLUSTER_TAG = "test_cluster";
    private static final String EXT_TAG = "test_ext_tag";
    private static final String PORT = "46801";
    private static final Long REPORT_TIME =
            LocalDate.now().atStartOfDay().toInstant(ZoneOffset.of("+8")).toEpochMilli();
    @BeforeAll
    void setup() {
        addHeartbeat();
    }
    private void addHeartbeat() {
        HeartbeatReportRequest request = new HeartbeatReportRequest();
        request.setIp(INSTANCE_IP);
        request.setComponentType(COMPONENT_TYPE);
        request.setReportTime(REPORT_TIME);
        List<GroupHeartbeat> groupList = new ArrayList<GroupHeartbeat>() {

            {
                add(new GroupHeartbeat(INLONG_GROUP_ID_1, STATUS, null));
                add(new GroupHeartbeat(INLONG_GROUP_ID_2, STATUS, null));
            }
        };
        request.setGroupHeartbeats(groupList);
        List<StreamHeartbeat> steamList = new ArrayList<StreamHeartbeat>() {

            {
                add(new StreamHeartbeat(INLONG_GROUP_ID_1, INLONG_STREAM_ID_1, STATUS, null));
                add(new StreamHeartbeat(INLONG_GROUP_ID_2, INLONG_STREAM_ID_2, STATUS, null));
            }
        };
        request.setStreamHeartbeats(steamList);
        request.setClusterName(CLUSTER_NAME);
        request.setClusterTag(CLUSTER_TAG);
        request.setExtTag(EXT_TAG);
        request.setPort(PORT);
        heartbeatService.reportHeartbeat(request);
    }

    @Test
    public void testGetComponentHeartbeat() throws Exception {

        // Mock the request and response
        HeartbeatQueryRequest request = new HeartbeatQueryRequest();
        request.setComponent(COMPONENT_TYPE);
        request.setInstance(INSTANCE_IP);

        ComponentHeartbeatResponse expect = new ComponentHeartbeatResponse();
        expect.setComponent(COMPONENT_TYPE);
        expect.setInstance(INSTANCE_IP);
        expect.setReportTime(REPORT_TIME);
        // Call the controller method
        MvcResult mvcResult = mockMvc.perform(
                MockMvcRequestBuilders.post("/api/heartbeat/component/get")
                        .content(JsonUtils.toJsonString(request))
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        Response<ComponentHeartbeatResponse> response = getResBody(mvcResult, ComponentHeartbeatResponse.class);
        // Verify the result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        ComponentHeartbeatResponse result = response.getData();
        // The program will modify the field modifyTime according to the process time, So this field can't assert
        result.setModifyTime(null);
        Assertions.assertEquals(expect, result);
    }

    @Test
    public void testGetGroupHeartbeat() throws Exception {
        // Mock the request and response
        HeartbeatQueryRequest request = new HeartbeatQueryRequest();
        request.setInlongGroupId(INLONG_GROUP_ID_1);
        request.setComponent(COMPONENT_TYPE);
        request.setInstance(INSTANCE_IP);

        GroupHeartbeatResponse expect = new GroupHeartbeatResponse();
        expect.setInlongGroupId(INLONG_GROUP_ID_1);
        expect.setStatusHeartbeat(STATUS);
        expect.setComponent(COMPONENT_TYPE);
        expect.setInstance(INSTANCE_IP);
        expect.setReportTime(REPORT_TIME);
        // Call the controller method
        MvcResult mvcResult = mockMvc.perform(
                MockMvcRequestBuilders.post("/api/heartbeat/group/get")
                        .content(JsonUtils.toJsonString(request))
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        Response<GroupHeartbeatResponse> response = getResBody(mvcResult, GroupHeartbeatResponse.class);
        // Verify the result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        GroupHeartbeatResponse result = response.getData();
        // The program will modify the field modifyTime according to the process time, So this field can't assert
        result.setModifyTime(null);
        Assertions.assertEquals(expect, result);
    }

    @Test
    public void testGetStreamHeartbeat() throws Exception {
        // Mock the request and response
        HeartbeatQueryRequest request = new HeartbeatQueryRequest();
        request.setInlongGroupId(INLONG_GROUP_ID_1);
        request.setInlongStreamId(INLONG_STREAM_ID_1);
        request.setComponent(COMPONENT_TYPE);
        request.setInstance(INSTANCE_IP);

        StreamHeartbeatResponse expect = new StreamHeartbeatResponse();
        expect.setInlongGroupId(INLONG_GROUP_ID_1);
        expect.setInlongStreamId(INLONG_STREAM_ID_1);
        expect.setStatusHeartbeat(STATUS);
        expect.setComponent(COMPONENT_TYPE);
        expect.setInstance(INSTANCE_IP);
        expect.setReportTime(REPORT_TIME);
        // Call the controller method
        MvcResult mvcResult = mockMvc.perform(
                MockMvcRequestBuilders.post("/api/heartbeat/stream/get")
                        .content(JsonUtils.toJsonString(request))
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        Response<StreamHeartbeatResponse> response = getResBody(mvcResult, StreamHeartbeatResponse.class);
        // Verify the result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        StreamHeartbeatResponse result = response.getData();
        // The program will modify the field modifyTime according to the process time, So this field can't assert
        result.setModifyTime(null);
        Assertions.assertEquals(expect, result);

    }

    @Test
    public void testListComponentHeartbeat() throws Exception {
        // Mock the request and response
        HeartbeatQueryRequest request = new HeartbeatQueryRequest();
        request.setComponent(COMPONENT_TYPE);
        request.setInstance(INSTANCE_IP);

        ComponentHeartbeatResponse expect = new ComponentHeartbeatResponse();
        expect.setComponent(COMPONENT_TYPE);
        expect.setInstance(INSTANCE_IP);
        expect.setReportTime(REPORT_TIME);

        PageResult<ComponentHeartbeatResponse> expectPage = new PageResult<>();
        List<ComponentHeartbeatResponse> dataList = Collections.singletonList(expect);
        expectPage.setList(dataList);
        // Call the controller method
        MvcResult mvcResult = mockMvc.perform(
                MockMvcRequestBuilders.post("/api/heartbeat/component/list")
                        .content(JsonUtils.toJsonString(request))
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        List<ComponentHeartbeatResponse> response = getResBodyPageList(mvcResult, ComponentHeartbeatResponse.class);
        // Verify the result
        Assertions.assertNotNull(response);
        // The program will modify the field modifyTime according to the process time, So this field can't assert
        response.forEach(c -> c.setModifyTime(null));
        Assertions.assertEquals(expectPage.getList(), response);
    }

    @Test
    public void testListGroupHeartbeat() throws Exception {
        // Mock the request and response
        HeartbeatQueryRequest request = new HeartbeatQueryRequest();
        request.setInlongGroupId(INLONG_GROUP_ID_1);
        request.setComponent(COMPONENT_TYPE);
        request.setInstance(INSTANCE_IP);

        GroupHeartbeatResponse expect = new GroupHeartbeatResponse();
        expect.setInlongGroupId(INLONG_GROUP_ID_1);
        expect.setStatusHeartbeat(STATUS);
        expect.setComponent(COMPONENT_TYPE);
        expect.setInstance(INSTANCE_IP);
        expect.setReportTime(REPORT_TIME);

        PageResult<GroupHeartbeatResponse> expectPage = new PageResult<>();
        List<GroupHeartbeatResponse> dataList = Collections.singletonList(expect);
        expectPage.setList(dataList);
        // Call the controller method
        MvcResult mvcResult = mockMvc.perform(
                MockMvcRequestBuilders.post("/api/heartbeat/group/list")
                        .content(JsonUtils.toJsonString(request))
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        List<GroupHeartbeatResponse> result = getResBodyPageList(mvcResult, GroupHeartbeatResponse.class);
        // Verify the result
        Assertions.assertNotNull(result);
        // The program will modify the field modifyTime according to the process time, So this field can't assert
        result.forEach(g -> g.setModifyTime(null));
        Assertions.assertEquals(expectPage.getList(), result);
    }

    @Test
    public void testListStreamHeartbeat() throws Exception {
        // Mock the request and response
        HeartbeatQueryRequest request = new HeartbeatQueryRequest();
        request.setInlongGroupId(INLONG_GROUP_ID_1);
        request.setInlongStreamId(INLONG_STREAM_ID_1);
        request.setComponent(COMPONENT_TYPE);
        request.setInstance(INSTANCE_IP);

        StreamHeartbeatResponse expect = new StreamHeartbeatResponse();
        expect.setInlongGroupId(INLONG_GROUP_ID_1);
        expect.setInlongStreamId(INLONG_STREAM_ID_1);
        expect.setStatusHeartbeat(STATUS);
        expect.setComponent(COMPONENT_TYPE);
        expect.setInstance(INSTANCE_IP);
        expect.setReportTime(REPORT_TIME);

        PageResult<StreamHeartbeatResponse> expectPage = new PageResult<>();
        List<StreamHeartbeatResponse> dataList = Collections.singletonList(expect);
        expectPage.setList(dataList);
        // Call the controller method
        MvcResult mvcResult = mockMvc.perform(
                MockMvcRequestBuilders.post("/api/heartbeat/stream/list")
                        .content(JsonUtils.toJsonString(request))
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        List<StreamHeartbeatResponse> result = getResBodyPageList(mvcResult, StreamHeartbeatResponse.class);
        // Verify the result
        Assertions.assertNotNull(result);
        // The program will modify the field modifyTime according to the process time, So this field can't assert
        result.forEach(s -> s.setModifyTime(null));
        Assertions.assertEquals(expectPage.getList(), result);
    }
}