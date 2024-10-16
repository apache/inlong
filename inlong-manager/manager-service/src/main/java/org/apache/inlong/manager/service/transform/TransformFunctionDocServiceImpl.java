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
import org.apache.inlong.sdk.transform.process.function.FunctionTools;
import org.apache.inlong.sdk.transform.process.pojo.FunctionInfo;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class TransformFunctionDocServiceImpl implements TransformFunctionDocService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformFunctionDocServiceImpl.class);

    private final Map<String, Set<FunctionInfo>> functionDocMap = FunctionTools.getFunctionDoc();

    @Override
    public PageResult<TransformFunctionDocResponse> listByCondition(TransformFunctionDocRequest request) {
        String type = request.getType();
        String name = request.getName();
        int pageNum = request.getPageNum();
        int pageSize = request.getPageSize();

        LOGGER.info("begin to query transform function info: {}", request);
        List<FunctionInfo> filteredFunctionInfos = functionDocMap.entrySet().stream()
                .filter(entry -> StringUtils.isBlank(type) || entry.getKey().equals(type))
                .flatMap(entry -> entry.getValue().stream())
                .filter(functionInfo -> StringUtils.isBlank(name)
                        || functionInfo.getFunctionName().toLowerCase().contains(name.toLowerCase()))
                .collect(Collectors.toList());

        return paginateFunctionInfos(filteredFunctionInfos, pageNum, pageSize);
    }

    private PageResult<TransformFunctionDocResponse> paginateFunctionInfos(List<FunctionInfo> functionInfos,
            int pageNum, int pageSize) {

        // pagination handle
        int totalItems = functionInfos.size();
        int startIndex = (pageNum - 1) * pageSize;
        int endIndex = Math.min(startIndex + pageSize, totalItems);

        if (startIndex >= totalItems) {
            LOGGER.error("error in transform function query pagination, startIndex: {}, totalItems: {}", startIndex,
                    totalItems);
            return PageResult.empty((long) totalItems);
        }
        List<FunctionInfo> pagedFunctionInfos = functionInfos.subList(startIndex, endIndex);

        List<TransformFunctionDocResponse> responseList = pagedFunctionInfos.stream()
                .map(functionInfo -> new TransformFunctionDocResponse(
                        functionInfo.getFunctionName(),
                        functionInfo.getExplanation(),
                        functionInfo.getExample()))
                .collect(Collectors.toList());
        LOGGER.info("transform function list query success");
        return new PageResult<>(responseList, (long) totalItems, pageNum, pageSize);
    }

}
