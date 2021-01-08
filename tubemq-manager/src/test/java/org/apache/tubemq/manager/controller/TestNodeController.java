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

import lombok.extern.slf4j.Slf4j;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.repository.NodeRepository;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.RequestBuilder;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
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
}
