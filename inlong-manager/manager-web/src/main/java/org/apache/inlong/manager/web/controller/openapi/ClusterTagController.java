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

import org.apache.inlong.manager.pojo.cluster.tag.topic.ClusterTagChangeRetentionRequest;
import org.apache.inlong.manager.pojo.cluster.tag.topic.ClusterTagCreateTopicRequest;
import org.apache.inlong.manager.pojo.cluster.tag.topic.ClusterTagDeleteTopicRequest;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.service.cluster.tag.topic.ClusterTagService;
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
@Api(tags = "Cluster-Tag-API")
public class ClusterTagController {

    @Autowired
    private ClusterTagService clusterTagService;

    @RequestMapping(value = "/cluster/tag/topic/create", method = RequestMethod.POST)
    @ApiOperation(value = "Create a topic in all message queue of Cluster Tag")
    public Response<String> createTopic(@RequestBody ClusterTagCreateTopicRequest request) {
        String result = clusterTagService.createTopic(request);
        if (result == null) {
            return Response.success();
        } else {
            return Response.fail(result);
        }
    }

    @RequestMapping(value = "/cluster/tag/topic/delete", method = RequestMethod.POST)
    @ApiOperation(value = "Delete a topic from all message queue of Cluster Tag")
    public Response<String> deleteTopic(@RequestBody ClusterTagDeleteTopicRequest request) {
        String result = clusterTagService.deleteTopic(request);
        if (result == null) {
            return Response.success();
        } else {
            return Response.fail(result);
        }
    }

    @RequestMapping(value = "/cluster/tag/topic/retention", method = RequestMethod.POST)
    @ApiOperation(value = "Change retention policies about all topics of this cluster tag")
    public Response<String> changeRetention(@RequestBody ClusterTagChangeRetentionRequest request) {
        String result = clusterTagService.changeRetention(request);
        if (result == null) {
            return Response.success();
        } else {
            return Response.fail(result);
        }
    }
}
