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

import org.apache.commons.text.similarity.FuzzyScore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class TransformFunctionDocServiceImpl implements TransformFunctionDocService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformFunctionDocServiceImpl.class);

    private final Map<String, Set<FunctionInfo>> functionDocMap = FunctionTools.getFunctionDoc();

    private final FuzzyScore fuzzyScore = new FuzzyScore(Locale.ENGLISH);

    private static final int FUZZY_THRESHOLD = 5;

    @Override
    public PageResult<TransformFunctionDocResponse> listByCondition(TransformFunctionDocRequest request) {
        String type = request.getType();
        String name = request.getName();
        int pageNum = request.getPageNum();
        int pageSize = request.getPageSize();

        LOGGER.info("begin to query transform function info: {}", request);
        List<FunctionInfo> filteredFunctionInfos = Optional.ofNullable(filterFunctionInfos(type, name, false))
                .filter(list -> !list.isEmpty())
                .orElseGet(() -> {
                    LOGGER.info("do not found transform function name: {}, fuzzy match enabled", name);
                    return filterFunctionInfos(type, name, true);
                });

        return paginateFunctionInfos(filteredFunctionInfos, pageNum, pageSize);
    }

    private List<FunctionInfo> filterFunctionInfos(String type, String name, boolean fuzzyMatch) {
        return functionDocMap.entrySet().stream()
                .filter(entry -> Optional.ofNullable(type)
                        .map(t -> entry.getKey().equals(t))
                        .orElse(true))
                .flatMap(entry -> entry.getValue().stream())
                .filter(functionInfo -> Optional.ofNullable(name)
                        .map(n -> fuzzyMatch ? isFuzzyMatch(n, functionInfo.getFunctionName())
                                : functionInfo.getFunctionName().toLowerCase().contains(n.toLowerCase()))
                        .orElse(true))
                .collect(Collectors.toList()).stream()
                .sorted(fuzzyMatch ?
                // strong correlation data first
                        Comparator.comparingInt((FunctionInfo f) -> fuzzyScore.fuzzyScore(f.getFunctionName(), name))
                                .reversed()
                        : Comparator.comparing(FunctionInfo::getFunctionName))
                .collect(Collectors.toList());
    }

    private boolean isFuzzyMatch(String input, String target) {
        return fuzzyScore.fuzzyScore(target, input) >= FUZZY_THRESHOLD;
    }

    private PageResult<TransformFunctionDocResponse> paginateFunctionInfos(List<FunctionInfo> functionInfos,
            int pageNum, int pageSize) {

        // pagination handle
        int totalItems = functionInfos.size();
        int startIndex = (pageNum - 1) * pageSize;
        int endIndex = Math.min(startIndex + pageSize, totalItems);

        return Optional.of(startIndex)
                .filter(index -> index < totalItems)
                .map(index -> {
                    List<TransformFunctionDocResponse> responseList =
                            functionInfos.subList(startIndex, endIndex).stream()
                                    .map(functionInfo -> new TransformFunctionDocResponse(
                                            functionInfo.getFunctionName(),
                                            functionInfo.getExplanation(),
                                            functionInfo.getExample()))
                                    .collect(Collectors.toList());

                    LOGGER.info("Transform function list query success. Total results: {}", totalItems);
                    return new PageResult<>(responseList, (long) totalItems, pageNum, pageSize);
                })
                .orElseGet(() -> {
                    LOGGER.error("Error in transform function query pagination, startIndex: {}, totalItems: {}",
                            startIndex, totalItems);
                    return PageResult.empty((long) totalItems);
                });
    }

}
