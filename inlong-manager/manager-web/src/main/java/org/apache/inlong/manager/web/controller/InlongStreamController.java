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

import com.github.pagehelper.PageInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.enums.OperationType;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamListResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamPageRequest;
import org.apache.inlong.manager.common.pojo.stream.FullPageUpdateRequest;
import org.apache.inlong.manager.common.pojo.stream.FullStreamRequest;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.StreamBriefResponse;
import org.apache.inlong.manager.common.util.LoginUserUtils;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.core.operationlog.OperationLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Inlong stream control layer
 */
@RestController
@RequestMapping("/stream")
@Api(tags = "Inlong stream config")
public class InlongStreamController {

    @Autowired
    private InlongStreamService streamService;

    @RequestMapping(value = "/save", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE)
    @ApiOperation(value = "Save inlong stream information")
    public Response<Integer> save(@RequestBody InlongStreamInfo streamInfo) {
        int result = streamService.save(streamInfo, LoginUserUtils.getLoginUserDetail().getUserName());
        return Response.success(result);
    }

    @RequestMapping(value = "/saveAll", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE)
    @ApiOperation(value = "Save inlong stream page information, including source and sink")
    public Response<Boolean> saveAll(@RequestBody FullStreamRequest pageInfo) {
        return Response.success(streamService.saveAll(pageInfo, LoginUserUtils.getLoginUserDetail().getUserName()));
    }

    @RequestMapping(value = "/batchSaveAll", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE)
    @ApiOperation(value = "Batch save inlong stream page information, including source and sink")
    public Response<Boolean> batchSaveAll(@RequestBody List<FullStreamRequest> infoList) {
        boolean result = streamService.batchSaveAll(infoList, LoginUserUtils.getLoginUserDetail().getUserName());
        return Response.success(result);
    }

    @RequestMapping(value = "/get", method = RequestMethod.GET)
    @ApiOperation(value = "Query inlong stream information")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "groupId", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "streamId", dataTypeClass = String.class, required = true)
    })
    public Response<InlongStreamInfo> get(@RequestParam String groupId, @RequestParam String streamId) {
        return Response.success(streamService.get(groupId, streamId));
    }

    @RequestMapping(value = "/list", method = RequestMethod.GET)
    @ApiOperation(value = "Paging query inlong stream list")
    public Response<PageInfo<InlongStreamListResponse>> listByCondition(InlongStreamPageRequest request) {
        request.setCurrentUser(LoginUserUtils.getLoginUserDetail().getUserName());
        return Response.success(streamService.listByCondition(request));
    }

    @RequestMapping(value = "/listAll", method = RequestMethod.GET)
    @ApiOperation(value = "Paging query all data of the inlong stream page under the specified groupId")
    public Response<PageInfo<FullStreamResponse>> listAllWithGroupId(InlongStreamPageRequest request) {
        request.setCurrentUser(LoginUserUtils.getLoginUserDetail().getUserName());
        return Response.success(streamService.listAllWithGroupId(request));
    }

    @RequestMapping(value = "/update", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Modify inlong stream information")
    public Response<Boolean> update(@RequestBody InlongStreamInfo streamInfo) {
        String username = LoginUserUtils.getLoginUserDetail().getUserName();
        return Response.success(streamService.update(streamInfo, username));
    }

    @RequestMapping(value = "/updateAll", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Modify inlong stream page information,including basic data source information")
    public Response<Boolean> updateAll(@RequestBody FullPageUpdateRequest updateInfo) {
        boolean result = streamService.updateAll(updateInfo, LoginUserUtils.getLoginUserDetail().getUserName());
        return Response.success(result);
    }

    @RequestMapping(value = "/delete", method = RequestMethod.DELETE)
    @OperationLog(operation = OperationType.DELETE)
    @ApiOperation(value = "Delete inlong stream information")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "inlongGroupId", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "streamId", dataTypeClass = String.class, required = true)
    })
    public Response<Boolean> delete(@RequestParam String inlongGroupId, @RequestParam String streamId) {
        String username = LoginUserUtils.getLoginUserDetail().getUserName();
        return Response.success(streamService.delete(inlongGroupId, streamId, username));
    }

    @RequestMapping(value = "/getSummaryList/{groupId}", method = RequestMethod.GET)
    @ApiOperation(value = "Obtain the flow of inlong stream according to groupId")
    @ApiImplicitParam(name = "groupId", value = "Inlong group id", dataTypeClass = String.class, required = true)
    public Response<List<StreamBriefResponse>> getSummaryList(@PathVariable String groupId) {
        return Response.success(streamService.getBriefList(groupId));
    }

}
