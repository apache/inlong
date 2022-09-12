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
import org.apache.inlong.manager.pojo.group.topic.InlongGroupChangeClusterTagRequest;
import org.apache.inlong.manager.pojo.group.topic.InlongGroupChangeNumPartitionsRequest;
import org.apache.inlong.manager.pojo.group.topic.InlongStreamChangeSortClusterRequest;
import org.apache.inlong.manager.service.group.topic.InlongGroupOpsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

/**
 * Message queue controller
 */
@RestController
@RequestMapping("/openapi")
@Api(tags = "Open-Group-API")
public class OpenGroupController {

    @Autowired
    private InlongGroupOpsService groupService;

    @RequestMapping(value = "/group/topic/change", method = RequestMethod.POST)
    @ApiOperation(value = "Change cluster tag of a inlong group id.")
    public Response<String> changeClusterTag(@RequestBody InlongGroupChangeClusterTagRequest request) {
        String result = groupService.changeClusterTag(request);
        if (result == null) {
            return Response.success();
        } else {
            return Response.fail(result);
        }
    }

    @RequestMapping(value = "/group/topic/partition", method = RequestMethod.POST)
    @ApiOperation(value = "Change Partition number of a inlong group id.")
    public Response<String> changeNumPartitions(@RequestBody InlongGroupChangeNumPartitionsRequest request) {
        String result = groupService.changeNumPartitions(request);
        if (result == null) {
            return Response.success();
        } else {
            return Response.fail(result);
        }
    }

    @RequestMapping(value = "/sink/cluster/change", method = RequestMethod.POST)
    @ApiOperation(value = "Change sort cluster of a inlong stream.")
    public Response<String> changeSortCluster(@RequestBody InlongStreamChangeSortClusterRequest request) {
        String result = groupService.changeSortCluster(request);
        if (result == null) {
            return Response.success();
        } else {
            return Response.fail(result);
        }
    }
}
