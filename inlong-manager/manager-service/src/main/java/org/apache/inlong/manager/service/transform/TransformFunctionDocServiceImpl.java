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

import org.apache.inlong.manager.pojo.transform.TransformFunctionDocInfo;
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
    public List<TransformFunctionDocResponse> getAllFunctionDocs() {

        return functionDocMap.entrySet().stream()
                .map(entry -> {
                    TransformFunctionDocResponse response = new TransformFunctionDocResponse();
                    response.setType(entry.getKey());

                    List<TransformFunctionDocInfo> docInfos = entry.getValue().stream()
                            .map(this::convertToDocInfo)
                            .collect(Collectors.toList());

                    response.setTransformFunctionDocInfoList(docInfos);
                    return response;
                })
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getAllFunctionTypes() {
        return new ArrayList<>(functionDocMap.keySet());
    }

    @Override
    public TransformFunctionDocResponse getFunctionDocsByType(String type) {
        List<FunctionInfo> functionInfos = functionDocMap.get(type);

        if (functionInfos != null && !functionInfos.isEmpty()) {
            TransformFunctionDocResponse response = new TransformFunctionDocResponse();
            response.setType(type);

            List<TransformFunctionDocInfo> docInfos = functionInfos.stream()
                    .map(this::convertToDocInfo)
                    .collect(Collectors.toList());

            response.setTransformFunctionDocInfoList(docInfos);
            return response;
        }
        return null;
    }

    // convert FunctionInfo to TransformFunctionDocInfo
    private TransformFunctionDocInfo convertToDocInfo(FunctionInfo functionInfo) {
        return new TransformFunctionDocInfo(
                functionInfo.getFunctionName(),
                functionInfo.getExplanation(),
                functionInfo.getExample());
    }

}
