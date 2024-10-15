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

import org.apache.inlong.manager.pojo.transform.TransformFunctionDocRequest;
import org.apache.inlong.manager.pojo.transform.TransformFunctionDocResponse;
import org.apache.inlong.sdk.transform.process.function.FunctionTools;
import org.apache.inlong.sdk.transform.process.pojo.FunctionInfo;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class TransformFunctionDocServiceImpl implements TransformFunctionDocService {

    private final Map<String, List<FunctionInfo>> functionDocMap = FunctionTools.getFunctionDoc();

    @Override
    public List<TransformFunctionDocResponse> getFunctionDocs(TransformFunctionDocRequest request) {
        String type = request.getType();
        int pageNum = request.getPageNum();
        int pageSize = request.getPageSize();

        // handle type filtering
        if (type != null && !type.isEmpty()) {
            List<FunctionInfo> functionInfos = functionDocMap.get(type);

            if (functionInfos == null || functionInfos.isEmpty()) {
                return new ArrayList<>();
            }
            return paginateFunctionInfos(functionInfos, pageNum, pageSize);
        }

        List<FunctionInfo> allFunctionInfos = functionDocMap.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());

        return paginateFunctionInfos(allFunctionInfos, pageNum, pageSize);
    }

    private List<TransformFunctionDocResponse> paginateFunctionInfos(List<FunctionInfo> functionInfos, int pageNum,
            int pageSize) {

        // pagination handle
        int totalItems = functionInfos.size();
        int startIndex = (pageNum - 1) * pageSize;
        int endIndex = Math.min(startIndex + pageSize, totalItems);

        if (startIndex >= totalItems) {
            return new ArrayList<>();
        }
        List<FunctionInfo> pagedFunctionInfos = functionInfos.subList(startIndex, endIndex);

        return pagedFunctionInfos.stream()
                .map(functionInfo -> new TransformFunctionDocResponse(
                        functionInfo.getFunctionName(),
                        functionInfo.getExplanation(),
                        functionInfo.getExample()))
                .collect(Collectors.toList());
    }

}
