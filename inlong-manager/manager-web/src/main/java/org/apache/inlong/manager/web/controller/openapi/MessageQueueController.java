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

import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueClearTopicRequest;
import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueControlRequest;
import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueOfflineRequest;
import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueOnlineRequest;
import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueSynchronizeTopicRequest;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.service.cluster.queue.MessageQueueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

/**
 * Message queue controller
 */
@RestController
@RequestMapping("/openapi")
@Api(tags = "Message-Queue-API")
public class MessageQueueController {

    @Autowired
    private MessageQueueService queueService;

    @PostMapping("/cluster/queue/control")
    @ApiOperation(value = "Control produce operation and consume operation of Inlong message queue cluster ")
    public Response<Void> control(@RequestBody MessageQueueControlRequest request) {
        queueService.control(request);
        return Response.success();
    }

    @PostMapping("/cluster/queue/online")
    @ApiOperation(value = "Build relationships between DataProxy cluster and MessageQueue cluster")
    public Response<Void> online(@RequestBody MessageQueueOnlineRequest request) {
        queueService.online(request);
        return Response.success();
    }

    @PostMapping("/cluster/queue/offline")
    @ApiOperation(value = "Remove relationships between DataProxy cluster and MessageQueue cluster")
    public Response<Void> offline(@RequestBody MessageQueueOfflineRequest request) {
        queueService.offline(request);
        return Response.success();
    }

    @PostMapping("/cluster/queue/topic/synchronize")
    @ApiOperation(value = "Synchronize all topic from cluster tag to message queue cluster")
    public Response<Void> synchronize(@RequestBody MessageQueueSynchronizeTopicRequest request) {
        queueService.synchronizeTopic(request);
        return Response.success();
    }

    @PostMapping("/cluster/queue/topic/clear")
    @ApiOperation(value = "Clear all topic from a message queue cluster")
    public Response<Void> clear(@RequestBody MessageQueueClearTopicRequest request) {
        queueService.clearTopic(request);
        return Response.success();
    }
}
