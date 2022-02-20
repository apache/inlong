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
import org.apache.inlong.manager.common.pojo.source.SourceFileBasicInfo;
import org.apache.inlong.manager.common.pojo.source.SourceFileDetailInfo;
import org.apache.inlong.manager.common.pojo.source.SourceFileDetailListVO;
import org.apache.inlong.manager.common.pojo.source.SourceFileDetailPageRequest;
import org.apache.inlong.manager.common.util.LoginUserUtils;
import org.apache.inlong.manager.service.core.SourceFileService;
import org.apache.inlong.manager.service.core.operationlog.OperationLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * File-based data source control layer
 */
@RestController
@RequestMapping("/datasource/file")
@Api(tags = "DataSource - File")
public class SourceFileController {

    @Autowired
    SourceFileService fileSourceService;

    @RequestMapping(value = "/saveBasic", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE)
    @ApiOperation(value = "Save basic information of file data source")
    public Response<Integer> saveBasic(@RequestBody SourceFileBasicInfo basicInfo) {
        int result = fileSourceService.saveBasic(basicInfo, LoginUserUtils.getLoginUserDetail().getUserName());
        return Response.success(result);
    }

    @RequestMapping(value = "/getBasic", method = RequestMethod.GET)
    @ApiOperation(value = "Query basic information of file data source")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "groupId", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "streamId", dataTypeClass = String.class, required = true)
    })
    public Response<SourceFileBasicInfo> getBasic(@RequestParam(name = "groupId") String groupId,
            @RequestParam(name = "streamId") String streamId) {
        return Response.success(fileSourceService.getBasicByIdentifier(groupId, streamId));
    }

    @RequestMapping(value = "/updateBasic", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Update basic information of file data source")
    public Response<Boolean> updateBasic(@RequestBody SourceFileBasicInfo basicInfo) {
        boolean result = fileSourceService.updateBasic(basicInfo, LoginUserUtils.getLoginUserDetail().getUserName());
        return Response.success(result);
    }

    @RequestMapping(value = "/deleteBasic/{id}", method = RequestMethod.DELETE)
    @OperationLog(operation = OperationType.DELETE)
    @ApiOperation(value = "Delete basic information of file data source")
    @ApiImplicitParam(name = "id", value = "File data source id", dataTypeClass = String.class, required = true)
    public Response<Boolean> deleteBasic(@PathVariable Integer id) {
        return Response
                .success(fileSourceService.logicDeleteBasic(id, LoginUserUtils.getLoginUserDetail().getUserName()));
    }

    @RequestMapping(value = "/saveDetail", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE)
    @ApiOperation(value = "Save file data source details")
    public Response<Integer> saveDetail(@RequestBody SourceFileDetailInfo detailInfo) {
        int result = fileSourceService.saveDetail(detailInfo, LoginUserUtils.getLoginUserDetail().getUserName());
        return Response.success(result);
    }

    @RequestMapping(value = "/getDetail/{id}", method = RequestMethod.GET)
    @ApiOperation(value = "Query file data source details")
    @ApiImplicitParam(name = "id", value = "id", dataTypeClass = String.class, required = true)
    public Response<SourceFileDetailInfo> getDetail(@PathVariable Integer id) {
        return Response.success(fileSourceService.getDetailById(id));
    }

    @RequestMapping(value = "/listDetail/", method = RequestMethod.GET)
    @ApiOperation(value = "Paging query file data source details")
    public Response<PageInfo<SourceFileDetailListVO>> listByCondition(SourceFileDetailPageRequest request) {
        return Response.success(fileSourceService.listByCondition(request));
    }

    @RequestMapping(value = "/updateDetail", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Modify file data source details")
    public Response<Boolean> updateDetail(@RequestBody SourceFileDetailInfo detailInfo) {
        boolean result = fileSourceService.updateDetail(detailInfo, LoginUserUtils.getLoginUserDetail().getUserName());
        return Response.success(result);
    }

    @RequestMapping(value = "/deleteDetail/{id}", method = RequestMethod.DELETE)
    @OperationLog(operation = OperationType.DELETE)
    @ApiOperation(value = "Delete file data source details")
    @ApiImplicitParam(name = "id", value = "File data source id", dataTypeClass = String.class, required = true)
    public Response<Boolean> deleteDetail(@PathVariable Integer id) {
        return Response
                .success(fileSourceService.logicDeleteDetail(id, LoginUserUtils.getLoginUserDetail().getUserName()));
    }

}