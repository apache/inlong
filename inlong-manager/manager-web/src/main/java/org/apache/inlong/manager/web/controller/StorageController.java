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
import org.apache.inlong.manager.common.pojo.datastorage.StorageListResponse;
import org.apache.inlong.manager.common.pojo.datastorage.StoragePageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageResponse;
import org.apache.inlong.manager.common.pojo.query.ColumnInfoBean;
import org.apache.inlong.manager.common.pojo.query.ConnectionInfo;
import org.apache.inlong.manager.common.pojo.query.DatabaseDetail;
import org.apache.inlong.manager.common.pojo.query.TableQueryBean;
import org.apache.inlong.manager.common.util.LoginUserUtil;
import org.apache.inlong.manager.service.core.DataSourceService;
import org.apache.inlong.manager.service.core.operationlog.OperationLog;
import org.apache.inlong.manager.service.storage.StorageService;
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
 * Data storage control layer
 */
@RestController
@RequestMapping("/storage")
@Api(tags = "Data Storage")
public class StorageController {

    @Autowired
    private DataSourceService dataSourceService;
    @Autowired
    private StorageService storageService;

    @RequestMapping(value = "/save", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE)
    @ApiOperation(value = "Save storage information")
    public Response<Integer> save(@Validated @RequestBody StorageRequest request) {
        return Response.success(storageService.save(request, LoginUserUtil.getLoginUserDetail().getUserName()));
    }

    @RequestMapping(value = "/get/{id}", method = RequestMethod.GET)
    @ApiOperation(value = "Query storage information")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", dataTypeClass = Integer.class, required = true),
            @ApiImplicitParam(name = "storageType", dataTypeClass = String.class, required = true)
    })
    public Response<StorageResponse> get(@PathVariable Integer id, @RequestParam String storageType) {
        return Response.success(storageService.get(id, storageType));
    }

    @RequestMapping(value = "/list", method = RequestMethod.GET)
    @ApiOperation(value = "Query data storage list based on conditions")
    public Response<PageInfo<? extends StorageListResponse>> listByCondition(StoragePageRequest request) {
        return Response.success(storageService.listByCondition(request));
    }

    @RequestMapping(value = "/update", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Modify data storage information")
    public Response<Boolean> update(@Validated @RequestBody StorageRequest request) {
        return Response.success(storageService.update(request, LoginUserUtil.getLoginUserDetail().getUserName()));
    }

    @RequestMapping(value = "/delete/{id}", method = RequestMethod.DELETE)
    @OperationLog(operation = OperationType.DELETE)
    @ApiOperation(value = "Delete data storage information")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", dataTypeClass = Integer.class, required = true),
            @ApiImplicitParam(name = "storageType", dataTypeClass = String.class, required = true)
    })
    public Response<Boolean> delete(@PathVariable Integer id, @RequestParam String storageType) {
        boolean result = storageService.delete(id, storageType, LoginUserUtil.getLoginUserDetail().getUserName());
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
