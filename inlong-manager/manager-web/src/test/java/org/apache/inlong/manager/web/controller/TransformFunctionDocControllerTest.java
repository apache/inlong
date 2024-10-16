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

import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.transform.TransformFunctionDocRequest;
import org.apache.inlong.manager.pojo.transform.TransformFunctionDocResponse;
import org.apache.inlong.manager.web.WebBaseTest;
import org.apache.inlong.sdk.transform.process.function.FunctionTools;
import org.apache.inlong.sdk.transform.process.pojo.FunctionInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.Map;
import java.util.Set;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class TransformFunctionDocControllerTest extends WebBaseTest {

    private final Map<String, Set<FunctionInfo>> expectfunctionDocMap = FunctionTools.getFunctionDoc();

    @SuppressWarnings("rawtypes")
    @Test
    public void testGetTransformFunctionDocs() throws Exception {

        // Mock the request and response
        TransformFunctionDocRequest request = new TransformFunctionDocRequest();
        PageResult<TransformFunctionDocResponse> expect = new PageResult<>();
        expect.setTotal(expectfunctionDocMap.values().stream().mapToLong(Set::size).sum());
        expect.setPageNum(1);
        expect.setPageSize(10);

        // Call the controller method
        MvcResult mvcResult = mockMvc.perform(
                MockMvcRequestBuilders.post("/openapi/transform/function/list")
                        .content(JsonUtils.toJsonString(request))
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
        Response<PageResult> response = getResBody(mvcResult, PageResult.class);

        // Verify the result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Long total = response.getData().getTotal();
        int pageSize = response.getData().getPageSize();
        int pageNum = response.getData().getPageNum();

        Assertions.assertEquals(expect.getTotal(), total);
        Assertions.assertEquals(expect.getPageNum(), pageNum);
        Assertions.assertEquals(expect.getPageSize(), pageSize);

    }

}
