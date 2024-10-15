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

import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.transform.TransformFunctionDocRequest;
import org.apache.inlong.manager.pojo.transform.TransformFunctionDocResponse;
import org.apache.inlong.manager.service.transform.TransformFunctionDocService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

import java.util.List;

/**
 * Open InLong transform function controller
 */
@RestController
@RequestMapping("/openapi/transform")
@Api(tags = "Open-TransformFunction-API")
public class OpenTransformFunctionController {

    @Resource
    private TransformFunctionDocService transformFunctionDocService;

    @RequestMapping(value = "/function/docs", method = RequestMethod.GET)
    @ApiOperation(value = "Get transform function docs list with optional type filtering and pagination")
    public Response<List<TransformFunctionDocResponse>> listDocs(TransformFunctionDocRequest request) {
        return Response.success(transformFunctionDocService.getFunctionDocs(request));
    }

}
