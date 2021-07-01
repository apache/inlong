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
import org.apache.inlong.manager.common.pojo.datasource.SourceDbBasicInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceDbDetailInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceDbDetailListVO;
import org.apache.inlong.manager.common.pojo.datasource.SourceDbDetailPageRequest;
import org.apache.inlong.manager.common.util.LoginUserUtil;
import org.apache.inlong.manager.service.core.SourceDbService;
import org.apache.inlong.manager.service.core.operationlog.OperationLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Control layer of DB type data source
 */
@RestController
@RequestMapping("/datasource/db")
@Api(tags = "DataSource - DB")
public class SourceDbController {

    @Autowired
    SourceDbService dbSourceService;

    @RequestMapping(value = "/saveBasic", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE)
    @ApiOperation(value = "Save the basic information of the DB data source")
    public Response<Integer> saveBasic(@RequestBody SourceDbBasicInfo basicInfo) {
        return Response.success(dbSourceService.saveBasic(basicInfo, LoginUserUtil.getLoginUserDetail().getUserName()));
    }

    @RequestMapping(value = "/getBasic", method = RequestMethod.GET)
    @ApiOperation(value = "Query basic information of DB data source")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "businessIdentifier", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "dataStreamIdentifier", dataTypeClass = String.class, required = true)
    })
    public Response<SourceDbBasicInfo> getBasic(@RequestParam(name = "businessIdentifier") String businessIdentifier,
            @RequestParam(name = "dataStreamIdentifier") String dataStreamIdentifier) {
        return Response.success(dbSourceService.getBasicByIdentifier(businessIdentifier, dataStreamIdentifier));
    }

    @RequestMapping(value = "/updateBasic", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Modify the basic information of the DB data source")
    public Response<Boolean> updateBasic(@RequestBody SourceDbBasicInfo basicInfo) {
        boolean result = dbSourceService.updateBasic(basicInfo, LoginUserUtil.getLoginUserDetail().getUserName());
        return Response.success(result);
    }

    @RequestMapping(value = "/deleteBasic/{id}", method = RequestMethod.DELETE)
    @OperationLog(operation = OperationType.DELETE)
    @ApiOperation(value = "Delete the basic information of the DB data source")
    @ApiImplicitParam(name = "id", value = "DB data source id", dataTypeClass = String.class, required = true)
    public Response<Boolean> deleteBasic(@PathVariable Integer id) {
        return Response.success(dbSourceService.logicDeleteBasic(id, LoginUserUtil.getLoginUserDetail().getUserName()));
    }

    @RequestMapping(value = "/saveDetail", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE)
    @ApiOperation(value = "Save DB data source details")
    public Response<Integer> saveDetail(@RequestBody SourceDbDetailInfo detailInfo) {
        return Response
                .success(dbSourceService.saveDetail(detailInfo, LoginUserUtil.getLoginUserDetail().getUserName()));
    }

    @RequestMapping(value = "/getDetail/{id}", method = RequestMethod.GET)
    @ApiOperation(value = "Query DB data source details")
    @ApiImplicitParam(name = "id", value = "id", dataTypeClass = String.class, required = true)
    public Response<SourceDbDetailInfo> getDetail(@PathVariable Integer id) {
        return Response.success(dbSourceService.getDetailById(id));
    }

    @RequestMapping(value = "/listDetail/", method = RequestMethod.GET)
    @ApiOperation(value = "Paging query DB data source details")
    public Response<PageInfo<SourceDbDetailListVO>> listByCondition(SourceDbDetailPageRequest request) {
        return Response.success(dbSourceService.listByCondition(request));
    }

    @RequestMapping(value = "/updateDetail", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Update DB data source details")
    public Response<Boolean> updateDetail(@RequestBody SourceDbDetailInfo detailInfo) {
        boolean result = dbSourceService.updateDetail(detailInfo, LoginUserUtil.getLoginUserDetail().getUserName());
        return Response.success(result);
    }

    @RequestMapping(value = "/deleteDetail/{id}", method = RequestMethod.DELETE)
    @OperationLog(operation = OperationType.DELETE)
    @ApiOperation(value = "Delete DB data source details")
    @ApiImplicitParam(name = "id", value = "DB data source id", dataTypeClass = String.class, required = true)
    public Response<Boolean> deleteDetail(@PathVariable Integer id) {
        return Response
                .success(dbSourceService.logicDeleteDetail(id, LoginUserUtil.getLoginUserDetail().getUserName()));
    }

}
