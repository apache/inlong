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

import com.github.pagehelper.Page;
import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.pojo.heartbeat.ComponentHeartbeatResponse;
import org.apache.inlong.manager.pojo.heartbeat.GroupHeartbeatResponse;
import org.apache.inlong.manager.pojo.heartbeat.HeartbeatPageRequest;
import org.apache.inlong.manager.pojo.heartbeat.HeartbeatQueryRequest;
import org.apache.inlong.manager.pojo.heartbeat.HeartbeatReportRequest;
import org.apache.inlong.manager.pojo.heartbeat.StreamHeartbeatResponse;
import com.google.common.collect.Maps;
import org.apache.inlong.common.heartbeat.GroupHeartbeat;
import org.apache.inlong.common.heartbeat.StreamHeartbeat;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.ComponentHeartbeatEntity;
import org.apache.inlong.manager.dao.entity.GroupHeartbeatEntity;
import org.apache.inlong.manager.dao.entity.StreamHeartbeatEntity;
import org.apache.inlong.manager.dao.mapper.ComponentHeartbeatEntityMapper;
import org.apache.inlong.manager.dao.mapper.GroupHeartbeatEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamHeartbeatEntityMapper;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.HeartbeatService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.BDDMockito.given;

/**
 * Heartbeat service test.
 */
@EnableAutoConfiguration
public class HeartbeatServiceTest extends ServiceBaseTest {

    @Autowired
    private HeartbeatService heartbeatService;
    @Autowired
    private ComponentHeartbeatEntityMapper componentHeartbeatMapper;
    @Autowired
    private GroupHeartbeatEntityMapper groupHeartbeatMapper;
    @Autowired
    private StreamHeartbeatEntityMapper streamHeartbeatMapper;

    /**
     * setUp
     */
//    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        ComponentHeartbeatEntity componentHeartbeat = new ComponentHeartbeatEntity();
        componentHeartbeat.setComponent("Sort");
        componentHeartbeat.setInstance("127.0.0.1");
        componentHeartbeat.setStatusHeartbeat("[{\"status\":\"running\"}]");
        componentHeartbeat.setMetricHeartbeat("[{\"mem\":\"16gb\",\"cpu\":\"60%\"}]");
        componentHeartbeat.setReportTime(System.currentTimeMillis());
        Page<ComponentHeartbeatEntity> componentPage = new Page<>();
        componentPage.add(componentHeartbeat);
        componentPage.setTotal(1);
        given(componentHeartbeatMapper.insert(new ComponentHeartbeatEntity())).willReturn(1);
        given(componentHeartbeatMapper.selectByKey(Mockito.anyString(),
                Mockito.anyString())).willReturn(componentHeartbeat);
        given(componentHeartbeatMapper.selectByCondition(Mockito.any())).willReturn(componentPage);

        GroupHeartbeatEntity groupHeartbeat = new GroupHeartbeatEntity();
        groupHeartbeat.setComponent("Sort");
        groupHeartbeat.setInstance("127.0.0.1");
        groupHeartbeat.setStatusHeartbeat("[{\"summaryMetric\":{\"totalRecordNumOfRead\""
                + ": \"10\"},\"streamMetrics\":[{\"streamId\":\"stream1\"}]}]");
        groupHeartbeat.setReportTime(System.currentTimeMillis());
        groupHeartbeat.setMetricHeartbeat("[{\"summaryMetric\":{\"totalRecordNumOfRead\""
                + ": \"10\"},"
                + "\"streamMetrics\":[{\"streamId\":\"stream1\"}]}]");
        Page<GroupHeartbeatEntity> groupPage = new Page<>();
        groupPage.add(groupHeartbeat);
        groupPage.setTotal(1);
        given(groupHeartbeatMapper.selectByKey(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString())).willReturn(groupHeartbeat);
        given(groupHeartbeatMapper.selectByCondition(Mockito.any())).willReturn(groupPage);

        StreamHeartbeatEntity streamHeartbeat = new StreamHeartbeatEntity();
        streamHeartbeat.setComponent("Sort");
        streamHeartbeat.setInstance("127.0.0.1");
        streamHeartbeat.setInlongGroupId("group1");
        streamHeartbeat.setInlongStreamId("test_test");
        streamHeartbeat.setStatusHeartbeat("[{\"statue\":\"running\"}]");
        streamHeartbeat.setMetricHeartbeat("[{\"outMsg\":\"1\",\"inMsg\":2}]");
        streamHeartbeat.setReportTime(System.currentTimeMillis());

        Page<StreamHeartbeatEntity> streamPage = new Page<>();
        streamPage.add(streamHeartbeat);
        streamPage.setTotal(1);

        given(streamHeartbeatMapper.selectByKey(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                .willReturn(streamHeartbeat);
        given(streamHeartbeatMapper.selectByCondition(Mockito.any())).willReturn(streamPage);
    }

    @Test
    public void testReportHeartbeat() {
        HeartbeatReportRequest request = new HeartbeatReportRequest();
        request.setComponentType("Sort");
        request.setIp("127.0.0.1");
        request.setReportTime(Instant.now().toEpochMilli());

        List<GroupHeartbeat> groupHeartbeats = new ArrayList<>();
        GroupHeartbeat groupHeartbeat = new GroupHeartbeat();
        groupHeartbeat.setInlongGroupId("group1");
        groupHeartbeat.setStatus("running");
        request.setGroupHeartbeats(groupHeartbeats);

        StreamHeartbeat streamHeartbeat = new StreamHeartbeat();
        Map<String, String> metrics = Maps.newHashMap();
        metrics.put("count", "10000");
        metrics.put("speed", "100/s");
        streamHeartbeat.setMetric(JsonUtils.toJsonString(metrics));
        streamHeartbeat.setStatus("running");
        streamHeartbeat.setInlongGroupId("group1");
        streamHeartbeat.setInlongStreamId("stream1");
        List<StreamHeartbeat> streamHeartbeats = new ArrayList<>();
        streamHeartbeats.add(streamHeartbeat);
        request.setStreamHeartbeats(streamHeartbeats);

        Assertions.assertTrue(heartbeatService.reportHeartbeat(request));
    }

    @Test
    public void testGetComponentHeartbeat() {
        HeartbeatQueryRequest request = new HeartbeatQueryRequest();
        request.setComponent("Sort");
        request.setInstance("127.0.0.1");
        ComponentHeartbeatResponse response = heartbeatService.getComponentHeartbeat(request);
        Assertions.assertEquals("127.0.0.1", response.getInstance());
    }

    @Test
    public void testGetGroupHeartbeat() {
        HeartbeatQueryRequest request = new HeartbeatQueryRequest();
        request.setComponent("Sort");
        request.setInstance("127.0.0.1");
        request.setInlongGroupId("group1");
        GroupHeartbeatResponse response = heartbeatService.getGroupHeartbeat(request);
        Assertions.assertEquals("127.0.0.1", response.getInstance());
    }

    @Test
    public void testGetStreamHeartbeat() {
        HeartbeatQueryRequest request = new HeartbeatQueryRequest();
        request.setComponent("Sort");
        request.setInstance("127.0.0.1");
        request.setInlongGroupId("group1");
        request.setInlongStreamId("stream1");

        StreamHeartbeatResponse response = heartbeatService.getStreamHeartbeat(request);
        Assertions.assertEquals("127.0.0.1", response.getInstance());
    }

    @Test
    public void testListComponentHeartbeat() {
        HeartbeatPageRequest request = new HeartbeatPageRequest();
        request.setComponent("Sort");
        request.setPageNum(1);
        request.setPageSize(10);
        PageInfo<ComponentHeartbeatResponse> pageResponse = heartbeatService.listComponentHeartbeat(request);
        Assertions.assertEquals(1, pageResponse.getTotal());
    }

    @Test
    public void testListGroupHeartbeat() {
        HeartbeatPageRequest request = new HeartbeatPageRequest();
        request.setComponent("Sort");
        request.setInstance("127.0.0.1");
        request.setPageNum(1);
        request.setPageSize(10);
        PageInfo<GroupHeartbeatResponse> pageResponse = heartbeatService.listGroupHeartbeat(request);
        Assertions.assertEquals(1, pageResponse.getTotal());
    }

    @Test
    public void testListStreamHeartbeat() {
        HeartbeatPageRequest request = new HeartbeatPageRequest();
        request.setComponent("Sort");
        request.setInstance("127.0.0.1");
        request.setInlongGroupId("group1");
        request.setPageNum(1);
        request.setPageSize(10);
        PageInfo<StreamHeartbeatResponse> pageResponse = heartbeatService.listStreamHeartbeat(request);
        Assertions.assertEquals(1, pageResponse.getTotal());
    }
}
