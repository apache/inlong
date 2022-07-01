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
import org.apache.inlong.manager.common.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.common.pojo.node.DataNodeResponse;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.web.WebBaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MvcResult;

import javax.annotation.Resource;
import java.util.Date;

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

        MvcResult mvcResult = postForSuccessMvcResult("/node/save", getDataNodeRequest());

        Response<Integer> response = getResBody(mvcResult, Integer.class);
        Assertions.assertEquals("Current user [operator] has no permission to access URL", response.getErrMsg());
    }

    @Test
    void testSaveAndGetAndDelete() throws Exception {
        // save
        MvcResult mvcResult = postForSuccessMvcResult("/node/save", getDataNodeRequest());

        Integer dataNodeId = getResBodyObj(mvcResult, Integer.class);
        Assertions.assertNotNull(dataNodeId);

        // get
        MvcResult getResult = getForSuccessMvcResult("/node/get/{id}", dataNodeId);

        DataNodeResponse dataNode = getResBodyObj(getResult, DataNodeResponse.class);
        Assertions.assertNotNull(dataNode);
        Assertions.assertEquals(getDataNodeRequest().getName(), dataNode.getName());

        // delete
        MvcResult deleteResult = deleteForSuccessMvcResult("/node/delete/{id}", dataNodeId);

        Boolean success = getResBodyObj(deleteResult, Boolean.class);
        Assertions.assertTrue(success);

        DataNodeEntity dataNodeEntity = dataNodeEntityMapper.selectById(dataNodeId);
        Assertions.assertEquals(dataNodeEntity.getId(), dataNodeEntity.getIsDeleted());
    }

    @Test
    void testUpdate() throws Exception {
        // insert the test data
        DataNodeEntity nodeEntity = new DataNodeEntity();
        nodeEntity.setName("test");
        nodeEntity.setType("MYSQL");
        nodeEntity.setIsDeleted(0);
        nodeEntity.setModifier("test");
        nodeEntity.setCreator("test");
        nodeEntity.setCreateTime(new Date());
        nodeEntity.setModifyTime(new Date());
        nodeEntity.setInCharges("test");

        dataNodeEntityMapper.insert(nodeEntity);

        DataNodeRequest request = getDataNodeRequest();
        request.setId(nodeEntity.getId());
        request.setName("test447777");

        MvcResult mvcResult = postForSuccessMvcResult("/node/update", request);

        Boolean success = getResBodyObj(mvcResult, Boolean.class);
        Assertions.assertTrue(success);

        DataNodeEntity dataNodeEntity = dataNodeEntityMapper.selectById(request.getId());
        Assertions.assertEquals(request.getName(), dataNodeEntity.getName());
    }

    @Test
    void testUpdateFailByNoId() throws Exception {
        MvcResult mvcResult = postForSuccessMvcResult("/node/update", getDataNodeRequest());

        Response<Boolean> response = getResBody(mvcResult, Boolean.class);
        Assertions.assertFalse(response.isSuccess());
        Assertions.assertEquals("id: must not be null\n", response.getErrMsg());
    }

}
