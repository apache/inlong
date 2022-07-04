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

package org.apache.inlong.manager.web.controller.openapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.common.pojo.sortstandalone.SortClusterResponse;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.SortClusterConfigEntity;
import org.apache.inlong.manager.dao.entity.SortTaskIdParamEntity;
import org.apache.inlong.manager.dao.entity.SortTaskSinkParamEntity;
import org.apache.inlong.manager.dao.mapper.SortClusterConfgiEntityMapper;
import org.apache.inlong.manager.dao.mapper.SortTaskIdParamEntityMapper;
import org.apache.inlong.manager.dao.mapper.SortTaskSinkParamEntityMapper;
import org.apache.inlong.manager.web.WebBaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.transaction.annotation.Transactional;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Test class for sort controller.
 */
@Slf4j
@TestInstance(Lifecycle.PER_CLASS)
class SortControllerTest extends WebBaseTest {

    // todo Service do not support insert method now, use mappers to insert data.
    @Autowired
    private SortTaskIdParamEntityMapper taskIdParamEntityMapper;

    @Autowired
    private SortTaskSinkParamEntityMapper taskSinkParamEntityMapper;

    @Autowired
    private SortClusterConfgiEntityMapper sortClusterConfgiEntityMapper;

    @BeforeAll
    void setUp() {
        taskIdParamEntityMapper.insert(this.prepareIdParamsEntity("testTask1", 1));
        taskIdParamEntityMapper.insert(this.prepareIdParamsEntity("testTask1", 2));
        taskIdParamEntityMapper.insert(this.prepareIdParamsEntity("testTask2", 1));
        taskSinkParamEntityMapper
                .insert(this.prepareSinkParamsEntity("testTask1", "kafka", 1));
        taskSinkParamEntityMapper
                .insert(this.prepareSinkParamsEntity("testTask2", "pulsar", 1));
        sortClusterConfgiEntityMapper.insert(this.prepareClusterConfigEntity("testTask1", "kafka"));
        sortClusterConfgiEntityMapper.insert(this.prepareClusterConfigEntity("testTask2", "pulsar"));
    }
    
    // @Test
    // @Transactional
    public void testErrorSinkType() throws Exception {
        sortClusterConfgiEntityMapper.insert(
                this.prepareClusterConfigEntity("testTask1", "error type"));
        RequestBuilder request =
                get("/openapi/sort/getClusterConfig")
                        .param("clusterName", "testCluster")
                        .param("md5", "testMd5");
        mockMvc.perform(request).andExpect(status().isOk()).andDo(print());
    }

    // @Test
    // @Transactional
    public void testEmptyClusterNameWhenGet() throws Exception {
        RequestBuilder request =
                get("/openapi/sort/getClusterConfig")
                        .param("clusterName", "  ")
                        .param("md5", "testMd5");
        mockMvc.perform(request).andExpect(status().isOk()).andDo(print());
    }

    private SortTaskIdParamEntity prepareIdParamsEntity(String task, int idx) {
        return SortTaskIdParamEntity.builder()
                .groupId(String.valueOf(idx))
                .streamId(String.valueOf(idx))
                .taskName(task)
                .paramKey("idParamKey " + idx)
                .paramValue("idParamValue " + idx)
                .build();
    }

    private SortClusterConfigEntity prepareClusterConfigEntity(String taskName, String sinkType) {
        return SortClusterConfigEntity.builder()
                .clusterName("testCluster")
                .taskName(taskName)
                .sinkType(sinkType)
                .build();
    }

    private SortTaskSinkParamEntity prepareSinkParamsEntity(String task, String sinkType, int idx) {
        return SortTaskSinkParamEntity.builder()
                .sinkType(sinkType)
                .paramKey("sinkParamKey " + idx)
                .paramValue("sinkParamValue " + idx)
                .taskName(task)
                .build();
    }

}
