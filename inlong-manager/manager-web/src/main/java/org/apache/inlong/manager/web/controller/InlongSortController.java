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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.inlong.manager.pojo.sort.ListSortStatusRequest;
import org.apache.inlong.manager.pojo.sort.ListSortStatusResponse;
import org.apache.inlong.manager.service.core.SortService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Inlong sort controller.
 */
@RestController
@RequestMapping("/api")
@Api(tags = "Inlong-Sort-API")
public class InlongSortController {

    @Autowired
    private SortService sortService;

    @RequestMapping(value = "/sort/listStatus", method = RequestMethod.POST)
    @ApiOperation(value = "List sort job status by inlong groups")
    public ListSortStatusResponse listSortStatus(@RequestBody ListSortStatusRequest request) {
        return sortService.listSortStatus(request);
    }

}
