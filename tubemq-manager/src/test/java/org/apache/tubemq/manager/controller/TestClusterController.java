/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.manager.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.tubemq.manager.controller.cluster.ClusterController;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.repository.NodeRepository;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.RequestBuilder;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
public class TestClusterController {

    private final Gson gson = new Gson();

    @MockBean
    private NodeRepository nodeRepository;

    @InjectMocks
    private ClusterController clusterController;

    @Autowired
    private MockMvc mockMvc;

    private NodeEntry getNodeEntry() {
        NodeEntry nodeEntry = new NodeEntry();
        nodeEntry.setMaster(true);
        nodeEntry.setIp("127.0.0.1");
        nodeEntry.setWebPort(8080);
        return nodeEntry;
    }

    @Test
    public void testExceptionQuery() throws Exception {
        NodeEntry nodeEntry = getNodeEntry();
        when(nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(any(Integer.class)))
                .thenReturn(nodeEntry);
        RequestBuilder request = get(
                "/v1/cluster/query?method=admin_query_topic_info&type=op_query");
        MvcResult result = mockMvc.perform(request).andReturn();
        String resultStr = result.getResponse().getContentAsString();
        TubeResult clusterResult = gson.fromJson(resultStr, TubeResult.class);
        Assert.assertEquals(-1, clusterResult.getErrCode());
        Assert.assertTrue(clusterResult.getErrMsg().contains("NumberFormatException"));
    }

    @Test
    public void testTopicQuery() throws Exception {
        NodeEntry nodeEntry = getNodeEntry();
        when(nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(any(Integer.class)))
                .thenReturn(nodeEntry);
        RequestBuilder request = get(
                "/v1/cluster/query?method=admin_query_topic_info&type=op_query&clusterId=1");
        MvcResult result = mockMvc.perform(request).andReturn();
        String resultStr = result.getResponse().getContentAsString();
        log.info("result json string is {}, response type is {}", resultStr,
                result.getResponse().getContentType());
    }

    @Test
    public void testBrokerQuery() throws Exception {
        NodeEntry nodeEntry = getNodeEntry();
        when(nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(any(Integer.class)))
                .thenReturn(nodeEntry);
        RequestBuilder request = get(
                "/v1/cluster/query?method=admin_query_broker_run_status&type=op_query&clusterId=1&brokerIp=");
        MvcResult result = mockMvc.perform(request).andReturn();
        String resultStr = result.getResponse().getContentAsString();
        log.info("result json string is {}, response type is {}", resultStr,
                result.getResponse().getContentType());
    }

    @Test
    public void testTopicAndGroupQuery() throws Exception {
        NodeEntry nodeEntry = getNodeEntry();
        when(nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(any(Integer.class)))
                .thenReturn(nodeEntry);
        RequestBuilder request = get(
                "/v1/cluster/query?method=admin_query_sub_info&type=op_query&clusterId=1&topicName=test&groupName=test");
        MvcResult result = mockMvc.perform(request).andReturn();
        String resultStr = result.getResponse().getContentAsString();
        log.info("result json string is {}, response type is {}", resultStr,
                result.getResponse().getContentType());
    }

    @Test
    public void testTopicAdd() throws Exception {
        String jsonStr = "{\n"
                + "  \"type\": \"op_modify\",\n"
                + "  \"method\": \"admin_add_new_topic_record\",\n"
                + "  \"confModAuthToken\": \"test\",\n"
                + "  \"clusterId\": 1,\n"
                + "  \"createUser\": \"webapi\",\n"
                + "  \"topicName\": \"test\",\n"
                + "  \"deleteWhen\": \"0 0 0 0 0\",\n"
                + "  \"unflushThreshold\": 1000,\n"
                + "  \"acceptPublish\": true,\n"
                + "  \"numPartitions\": 3,\n"
                + "  \"deletePolicy\": \"\",\n"
                + "  \"unflushInterval\": 1000,\n"
                + "  \"acceptSubscribe\": true,\n"
                + "  \"brokerId\": 12323\n"
                + "}\n";
        NodeEntry nodeEntry = getNodeEntry();
        when(nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(any(Integer.class)))
                .thenReturn(nodeEntry);
        RequestBuilder request = post("/v1/cluster/modify")
                .contentType(MediaType.APPLICATION_JSON).content(jsonStr);
        MvcResult result = mockMvc.perform(request).andReturn();
        String resultStr = result.getResponse().getContentAsString();
        log.info("result json string is {}, response type is {}", resultStr,
                result.getResponse().getContentType());
    }
}
