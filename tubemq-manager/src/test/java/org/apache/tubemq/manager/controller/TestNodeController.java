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

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.repository.NodeRepository;
import org.apache.tubemq.manager.service.NodeService;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.RequestBuilder;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
public class TestNodeController {

    @MockBean
    private NodeRepository nodeRepository;

    @Autowired
    private MockMvc mockMvc;

    private List<NodeEntry> getOneNodeEntry() {
        NodeEntry nodeEntry = new NodeEntry();
        nodeEntry.setMaster(true);
        nodeEntry.setIp("127.0.0.1");
        nodeEntry.setWebPort(8014);
        nodeEntry.setClusterId(0);
        return Lists.newArrayList(nodeEntry);
    }


    private List<NodeEntry> getTwoNodeEntries() {
        NodeEntry nodeEntry1 = new NodeEntry();
        nodeEntry1.setMaster(true);
        nodeEntry1.setIp("127.0.0.1");
        nodeEntry1.setWebPort(8014);
        nodeEntry1.setClusterId(1);

        NodeEntry nodeEntry2 = new NodeEntry();
        nodeEntry2.setMaster(true);
        nodeEntry2.setIp("127.0.0.1");
        nodeEntry2.setWebPort(8014);
        nodeEntry2.setClusterId(2);

        return Lists.newArrayList(nodeEntry1, nodeEntry2);
    }

    private String expectedOneEntry =
            "{\"data\":[{\"clusterId\":0," +
            "\"clusterInfo\":{\"master\":\"127.0.0.1\"," +
            "\"standby\":[],\"broker\":[]}}],\"errMsg\":" +
            "\"\",\"errCode\":0,\"result\":true}";


    private String expectedTwoEntries =
            "{\"data\":[{\"clusterId\":1,\"clusterInfo\":" +
                    "{\"master\":\"127.0.0.1\",\"standby\"" +
                    ":[],\"broker\":[]}},{\"clusterId\":2," +
                    "\"clusterInfo\":{\"master\":\"127.0.0.1\"," +
                    "\"standby\":[],\"broker\":[]}}]," +
                    "\"errMsg\":\"\",\"errCode\":0,\"result\":true}";

    private NodeEntry getNodeEntry() {
        NodeEntry nodeEntry = new NodeEntry();
        nodeEntry.setMaster(true);
        nodeEntry.setIp("127.0.0.1");
        nodeEntry.setWebPort(8084);
        return nodeEntry;
    }

    @Test
    public void testClusterInfo() throws Exception {
        List<NodeEntry> nodeEntries = getOneNodeEntry();
        when(nodeRepository.findNodeEntriesByClusterIdIs(any(Integer.class)))
                .thenReturn(nodeEntries);
        RequestBuilder request = get("/v1/node/query?method=admin_query_cluster_info&" +
                "type=op_query&clusterId=1");
        MvcResult result = mockMvc.perform(request).andReturn();
        String resultStr = result.getResponse().getContentAsString();
        Assert.assertEquals(resultStr, expectedOneEntry);
        log.info("result json string is {}, response type is {}", resultStr,
                result.getResponse().getContentType());
    }


    @Test
    public void testClusterInfoTwoEntries() throws Exception {
        List<NodeEntry> nodeEntries = getTwoNodeEntries();
        when(nodeRepository.findNodeEntriesByClusterIdIs(any(Integer.class)))
                .thenReturn(nodeEntries);
        RequestBuilder request = get("/v1/node/query?method=admin_query_cluster_info&" +
                "type=op_query&clusterId=1");
        MvcResult result = mockMvc.perform(request).andReturn();
        String resultStr = result.getResponse().getContentAsString();
        Assert.assertEquals(resultStr, expectedTwoEntries);
        log.info("result json string is {}, response type is {}", resultStr,
                result.getResponse().getContentType());
    }

    @Test
    public void testAddBrokersToCluster() throws Exception {
        String jsonStr = "{\n" +
                "\t\"confModAuthToken\": \"abc\",\n" +
                "\t\"createUser\": \"test\",\n" +
                "\t\"clusterId\": 1,\n" +
                "\t\"method\": \"admin_bath_add_broker_configure\",\n" +
                "\t\"type\": \"op_modify\",\n" +
                "\t\"brokerJsonSet\": [{\n" +
                "\t\t\"brokerId\": 234,\n" +
                "\t\t\"brokerIp\": \"127.0 .0 .1\",\n" +
                "\t\t\"brokerPort\": 8124,\n" +
                "\t\t\"numPartitions\": 3,\n" +
                "\t\t\"unflushThreshold\": 55,\n" +
                "\t\t\"unflushInterval\": 10000,\n" +
                "\t\t\"deleteWhen\": \"0 0 6,18 * * ?\",\n" +
                "\t\t\"deletePolicy\": \"delete,168\",\n" +
                "\t\t\"acceptPublish\": \"true\",\n" +
                "\t\t\"acceptSubscribe\": \"true\",\n" +
                "\t\t\"createUser\": \"gosonzhang\",\n" +
                "\t\t\"createDate\": \"20151116142135\",\n" +
                "\t\t\"modifyUser\": \"gosonzhang\",\n" +
                "\t\t\"modifyDate\": \"20151117161515\"\n" +
                "\t}]\n" +
                "\n" +
                "}";
        NodeEntry nodeEntry = getNodeEntry();
        doReturn(nodeEntry).when(nodeRepository).findNodeEntryByClusterIdIsAndMasterIsTrue(any(Integer.class));
        RequestBuilder request = post("/v1/node/add")
                .contentType(MediaType.APPLICATION_JSON).content(jsonStr);
        MvcResult result = mockMvc.perform(request).andReturn();
        String resultStr = result.getResponse().getContentAsString();
        log.info("result json string is {}, response type is {}", resultStr,
                result.getResponse().getContentType());
        }
    }
