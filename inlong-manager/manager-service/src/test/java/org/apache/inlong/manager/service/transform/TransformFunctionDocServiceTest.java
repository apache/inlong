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
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.FunctionTools;
import org.apache.inlong.sdk.transform.process.pojo.FunctionInfo;

import org.apache.commons.text.similarity.FuzzyScore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class TransformFunctionDocServiceTest extends ServiceBaseTest {

    @Autowired
    private TransformFunctionDocService transformFunctionDocService;

    private final Map<String, Set<FunctionInfo>> expectfunctionDocMap = FunctionTools.getFunctionDoc();

    private final FuzzyScore fuzzyScore = new FuzzyScore(Locale.ENGLISH);

    private static final int FUZZY_THRESHOLD = 3;

    @Test
    public void testTransformFunctionDoc() {
        TransformFunctionDocRequest request = new TransformFunctionDocRequest();

        PageResult<TransformFunctionDocResponse> functionDocs1 = transformFunctionDocService.listByCondition(request);
        // default pageNum = 1 pageSize = 10 (by PageResult)
        Assertions.assertEquals(10, functionDocs1.getList().size());
        Assertions.assertEquals(expectfunctionDocMap.values().stream().mapToLong(Set::size).sum(),
                functionDocs1.getTotal());
    }

    @Test
    public void testTransformFunctionDocPagination() {
        int expectTotal;
        TransformFunctionDocRequest request = new TransformFunctionDocRequest();
        request.setType(FunctionConstant.TEMPORAL_TYPE);
        PageResult<TransformFunctionDocResponse> functionDocs = transformFunctionDocService.listByCondition(request);

        expectTotal = (int) expectfunctionDocMap.entrySet().stream()
                .filter(entry -> entry.getKey().equals(request.getType()))
                .mapToLong(entry -> entry.getValue().size())
                .sum();
        Assertions.assertEquals(expectTotal, functionDocs.getTotal());

        request.setType(FunctionConstant.JSON_TYPE);
        request.setPageSize(5);
        request.setPageNum(2);
        PageResult<TransformFunctionDocResponse> functionDocs1 = transformFunctionDocService.listByCondition(request);

        Assertions.assertEquals(5, functionDocs1.getList().size());
        expectTotal = (int) expectfunctionDocMap.entrySet().stream()
                .filter(entry -> entry.getKey().equals(request.getType()))
                .mapToLong(entry -> entry.getValue().size())
                .sum();
        Assertions.assertEquals(expectTotal, functionDocs1.getTotal());

        request.setPageNum(1);
        PageResult<TransformFunctionDocResponse> functionDocs2 = transformFunctionDocService.listByCondition(request);
        Assertions.assertNotEquals(functionDocs1.getList(), functionDocs2.getList());
        // out of page
        request.setType(FunctionConstant.COMPRESSION_TYPE);
        request.setPageNum(2);
        request.setPageSize(2);
        PageResult<TransformFunctionDocResponse> functionDocs3 = transformFunctionDocService.listByCondition(request);

        Assertions.assertEquals(0, functionDocs3.getList().size());

        expectTotal = (int) expectfunctionDocMap.entrySet().stream()
                .filter(entry -> entry.getKey().equals(request.getType()))
                .mapToLong(entry -> entry.getValue().size())
                .sum();
        Assertions.assertEquals(expectTotal, functionDocs3.getTotal());
    }

    @Test
    public void testTransformFunctionDocByName() {
        TransformFunctionDocRequest request = new TransformFunctionDocRequest();
        request.setType(FunctionConstant.JSON_TYPE);
        request.setName("json_array");

        PageResult<TransformFunctionDocResponse> functionDocs = transformFunctionDocService.listByCondition(request);

        Assertions.assertEquals(3, functionDocs.getList().size());
        int expectTotal = (int) expectfunctionDocMap.entrySet().stream()
                .filter(entry -> entry.getKey().equals(request.getType()))
                .flatMap(entry -> entry.getValue().stream())
                .filter(functionInfo -> functionInfo.getFunctionName().toLowerCase()
                        .contains(request.getName().toLowerCase()))
                .count();
        Assertions.assertEquals(expectTotal, functionDocs.getTotal());
    }

    @Test
    public void testTransformFunctionDocByNameFuzzy() {
        TransformFunctionDocRequest request = new TransformFunctionDocRequest();
        request.setType(FunctionConstant.JSON_TYPE);
        request.setName("json_aray");

        PageResult<TransformFunctionDocResponse> functionDocs = transformFunctionDocService.listByCondition(request);

        Assertions.assertFalse(functionDocs.getList().isEmpty());
    }

}
