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

package org.apache.inlong.manager.service.transform;

import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.transform.TransformFunctionDocRequest;
import org.apache.inlong.manager.pojo.transform.TransformFunctionDocResponse;
import org.apache.inlong.manager.service.ServiceBaseTest;

import com.google.gson.Gson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class TransformFunctionDocServiceTest extends ServiceBaseTest {

    @Autowired
    private TransformFunctionDocService transformFunctionDocService;

    @Test
    public void testTransformFunctionDoc() {
        TransformFunctionDocRequest request = new TransformFunctionDocRequest();

        PageResult<TransformFunctionDocResponse> functionDocs1 = transformFunctionDocService.getFunctionDocs(request);
        // default pageNum = 1 pageSize = 10 (by PageResult)
        Assertions.assertEquals(10, functionDocs1.getList().size());
        Assertions.assertEquals(185, functionDocs1.getTotal());
    }

    @Test
    public void testTransformFunctionDocPagination() {
        TransformFunctionDocRequest request = new TransformFunctionDocRequest();
        request.setType("temporal");
        PageResult<TransformFunctionDocResponse> functionDocs = transformFunctionDocService.getFunctionDocs(request);

        Assertions.assertEquals(27, functionDocs.getTotal());

        request.setType("json");
        request.setPageSize(5);
        request.setPageNum(2);
        PageResult<TransformFunctionDocResponse> functionDocs1 = transformFunctionDocService.getFunctionDocs(request);

        Assertions.assertEquals(5, functionDocs1.getList().size());
        Assertions.assertEquals(12, functionDocs1.getTotal());

        request.setPageNum(1);
        PageResult<TransformFunctionDocResponse> functionDocs2 = transformFunctionDocService.getFunctionDocs(request);
        Assertions.assertNotEquals(functionDocs1.getList(), functionDocs2.getList());
        // out of page
        request.setType("compression");
        request.setPageNum(2);
        request.setPageSize(2);
        PageResult<TransformFunctionDocResponse> functionDocs3 = transformFunctionDocService.getFunctionDocs(request);

        Assertions.assertEquals(0, functionDocs3.getList().size());
        Assertions.assertEquals(2, functionDocs3.getTotal());
    }

    @Test
    public void testTransformFunctionDocByName() {
        TransformFunctionDocRequest request = new TransformFunctionDocRequest();
        request.setType("json");
        request.setName("json_array");

        PageResult<TransformFunctionDocResponse> functionDocs = transformFunctionDocService.getFunctionDocs(request);
        // default pageNum = 1 pageSize = 10 (by PageResult)
        Assertions.assertEquals(3, functionDocs.getList().size());
        Gson gson = new Gson();
        System.out.println(gson.toJson(functionDocs.getList()));
        Assertions.assertEquals(3, functionDocs.getTotal());
    }

}
