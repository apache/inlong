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
import org.apache.inlong.manager.common.pojo.query.ColumnInfoBean;
import org.apache.inlong.manager.common.pojo.query.ConnectionInfo;
import org.apache.inlong.manager.common.pojo.query.DatabaseDetail;
import org.apache.inlong.manager.common.pojo.query.TableQueryBean;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkPageRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.util.LoginUserUtils;
import org.apache.inlong.manager.service.core.DataSourceService;
import org.apache.inlong.manager.service.core.operationlog.OperationLog;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Stream sink control layer
 */
@RestController
@RequestMapping("/sink")
@Api(tags = "Stream sink config")
public class StreamSinkController {

    @Autowired
    private DataSourceService dataSourceService;
    @Autowired
    private StreamSinkService sinkService;

    @RequestMapping(value = "/save", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE)
    @ApiOperation(value = "Save sink information")
    public Response<Integer> save(@Validated @RequestBody SinkRequest request) {
        return Response.success(sinkService.save(request, LoginUserUtils.getLoginUserDetail().getUserName()));
    }

    @RequestMapping(value = "/get/{id}", method = RequestMethod.GET)
    @ApiOperation(value = "Query sink information")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", dataTypeClass = Integer.class, required = true),
            @ApiImplicitParam(name = "sinkType", dataTypeClass = String.class, required = true)
    })
    public Response<SinkResponse> get(@PathVariable Integer id, @RequestParam String sinkType) {
        return Response.success(sinkService.get(id, sinkType));
    }

    @RequestMapping(value = "/list", method = RequestMethod.GET)
    @ApiOperation(value = "Query stream sink list based on conditions")
    public Response<PageInfo<? extends SinkListResponse>> listByCondition(SinkPageRequest request) {
        return Response.success(sinkService.listByCondition(request));
    }

    @RequestMapping(value = "/update", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Modify data sink information")
    public Response<Boolean> update(@Validated @RequestBody SinkRequest request) {
        return Response.success(sinkService.update(request, LoginUserUtils.getLoginUserDetail().getUserName()));
    }

    @RequestMapping(value = "/delete/{id}", method = RequestMethod.DELETE)
    @OperationLog(operation = OperationType.DELETE)
    @ApiOperation(value = "Delete data sink information")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", dataTypeClass = Integer.class, required = true),
            @ApiImplicitParam(name = "sinkType", dataTypeClass = String.class, required = true)
    })
    public Response<Boolean> delete(@PathVariable Integer id, @RequestParam String sinkType) {
        boolean result = sinkService.delete(id, sinkType, LoginUserUtils.getLoginUserDetail().getUserName());
        return Response.success(result);
    }

    @RequestMapping(value = "/query/testConnection", method = RequestMethod.POST)
    @ApiOperation(value = "Test the connection")
    public Response<Boolean> testConnection(@RequestBody ConnectionInfo connectionInfo) {
        return Response.success(dataSourceService.testConnection(connectionInfo));
    }

    @RequestMapping(value = "/query/createDb", method = RequestMethod.POST)
    @ApiOperation(value = "Create database if not exists")
    public Response<Object> createDb(@RequestBody TableQueryBean queryBean) throws Exception {
        dataSourceService.createDb(queryBean);
        return Response.success();
    }

    @RequestMapping(value = "/query/columns", method = RequestMethod.POST)
    @ApiOperation(value = "Query table columns")
    public Response<List<ColumnInfoBean>> queryColumns(@RequestBody TableQueryBean queryBean) throws Exception {
        return Response.success(dataSourceService.queryColumns(queryBean));
    }

    @RequestMapping(value = "/query/dbDetail", method = RequestMethod.POST)
    @ApiOperation(value = "Query database detail")
    public Response<DatabaseDetail> queryDbDetail(@RequestBody TableQueryBean queryBean) throws Exception {
        return Response.success(dataSourceService.queryDbDetail(queryBean));
    }

}
