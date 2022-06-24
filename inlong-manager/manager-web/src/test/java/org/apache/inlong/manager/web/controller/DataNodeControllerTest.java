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

import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.enums.GlobalConstants;
import org.apache.inlong.manager.common.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.common.pojo.node.DataNodeResponse;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.web.WebBaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;

import javax.annotation.Resource;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class DataNodeControllerTest extends WebBaseTest {

    @Resource
    DataNodeEntityMapper dataNodeEntityMapper;

    DataNodeRequest getDataNodeRequest() {
        return DataNodeRequest.builder()
                .name("hiveNode1")
                .type("HIVE")
                .url("127.0.0.1:8080")
                .username("admin")
                .token("123")
                .inCharges("admin")
                .build();
    }

    @Test
    void testSaveFailByNoPermission() throws Exception {
        logout();
        operatorLogin();

        MvcResult mvcResult = mockMvc.perform(
                        post("/node/save")
                                .content(JsonUtils.toJsonString(getDataNodeRequest()))
                                .contentType(MediaType.APPLICATION_JSON_UTF8)
                                .accept(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk())
                .andReturn();

        Response<Integer> response = getResBody(mvcResult, Integer.class);
        Assertions.assertEquals("Current user [operator] has no permission to access URL", response.getErrMsg());
    }

    @Test
    void testSaveAndGetAndDelete() throws Exception {
        // save
        MvcResult mvcResult = mockMvc.perform(
                        post("/node/save")
                                .content(JsonUtils.toJsonString(getDataNodeRequest()))
                                .contentType(MediaType.APPLICATION_JSON_UTF8)
                                .accept(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk())
                .andReturn();

        Integer dataNodeId = getResBodyObj(mvcResult, Integer.class);
        Assertions.assertNotNull(dataNodeId);

        // get
        MvcResult getResult = mockMvc.perform(
                        get("/node/get/{id}", dataNodeId)
                                .contentType(MediaType.APPLICATION_JSON_UTF8)
                                .accept(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk())
                .andReturn();

        DataNodeResponse dataNode = getResBodyObj(getResult, DataNodeResponse.class);
        Assertions.assertNotNull(dataNode);
        Assertions.assertEquals(getDataNodeRequest().getName(), dataNode.getName());

        // delete
        MvcResult deleteResult = mockMvc.perform(
                        delete("/node/delete/{id}", dataNodeId)
                                .contentType(MediaType.APPLICATION_JSON_UTF8)
                                .accept(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk())
                .andReturn();

        Boolean success = getResBodyObj(deleteResult, Boolean.class);
        Assertions.assertTrue(success);

        DataNodeEntity dataNodeEntity = dataNodeEntityMapper.selectById(dataNodeId);
        Assertions.assertEquals(GlobalConstants.IS_DELETED, dataNodeEntity.getIsDeleted());
    }

    @Test
    void testUpdate() throws Exception {
        DataNodeRequest request = getDataNodeRequest();
        request.setId(1);
        request.setName("test447777");
        MvcResult mvcResult = mockMvc.perform(
                        post("/node/update")
                                .content(JsonUtils.toJsonString(request))
                                .contentType(MediaType.APPLICATION_JSON_UTF8)
                                .accept(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk())
                .andReturn();

        Boolean success = getResBodyObj(mvcResult, Boolean.class);
        Assertions.assertTrue(success);

        DataNodeEntity dataNodeEntity = dataNodeEntityMapper.selectById(request.getId());
        Assertions.assertEquals(request.getName(), dataNodeEntity.getName());
    }

    @Test
    void testUpdateFailByNoId() throws Exception {
        MvcResult mvcResult = mockMvc.perform(
                        post("/node/update")
                                .content(JsonUtils.toJsonString(getDataNodeRequest()))
                                .contentType(MediaType.APPLICATION_JSON_UTF8)
                                .accept(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk())
                .andReturn();

        Response<Boolean> response = getResBody(mvcResult, Boolean.class);
        Assertions.assertFalse(response.isSuccess());
        Assertions.assertEquals("id: must not be null\n", response.getErrMsg());
    }

}
