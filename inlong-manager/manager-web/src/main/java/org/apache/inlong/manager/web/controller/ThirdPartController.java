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
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeConsumeGroupRequest;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeMqTopicRequest;
import org.apache.inlong.manager.common.pojo.user.UserDetail;
import org.apache.inlong.manager.service.thirdpart.mq.TubeMqOptService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Associated system controller
 */
@RestController
@RequestMapping("/thirdpart")
@Api(tags = "Third Part System")
public class ThirdPartController {

    @Autowired
    private TubeMqOptService tubeMqOptService;

    @PostMapping("/createTopic")
    @ApiOperation(value = "Create tube topic")
    public Response<UserDetail> createTopic(@RequestBody AddTubeMqTopicRequest request) {
        try {
            tubeMqOptService.createNewTopic(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.success();
    }

    @PostMapping("/createGroup")
    @ApiOperation(value = "Create a tube consumer group")
    public Response<UserDetail> createGroup(@RequestBody AddTubeConsumeGroupRequest request) throws Exception {
        tubeMqOptService.createNewConsumerGroup(request);
        return Response.success();
    }

}
