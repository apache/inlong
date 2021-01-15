/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.tubemq.manager.controller.topic;

import static org.apache.tubemq.manager.service.TubeMQHttpConst.ADD;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.AUTH_CONTROL;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.CLONE;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.DELETE;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.MODIFY;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.REMOVE;
import static org.apache.tubemq.manager.service.MasterService.queryMaster;


import com.google.gson.Gson;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.controller.node.request.BatchAddTopicReq;
import org.apache.tubemq.manager.controller.node.request.CloneTopicReq;
import org.apache.tubemq.manager.controller.topic.request.DeleteTopicReq;
import org.apache.tubemq.manager.controller.topic.request.ModifyTopicReq;
import org.apache.tubemq.manager.controller.topic.request.SetAuthControlReq;
import org.apache.tubemq.manager.service.NodeService;
import org.apache.tubemq.manager.service.MasterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/v1/topic")
@Slf4j
public class TopicWebController {

    @Autowired
    private NodeService nodeService;

    private Gson gson = new Gson();

    @Autowired
    private MasterService masterService;

    /**
     * broker method proxy
     * divides the operation on broker to different method
     */
    @RequestMapping(value = "")
    public @ResponseBody TubeMQResult topicMethodProxy(
        @RequestParam String method, @RequestBody String req) throws Exception {
        switch (method) {
            case ADD:
                return nodeService.batchAddTopic(gson.fromJson(req, BatchAddTopicReq.class));
            case CLONE:
                return nodeService.cloneTopicToBrokers(gson.fromJson(req, CloneTopicReq.class));
            case AUTH_CONTROL:
                return masterService.baseRequestMaster(gson.fromJson(req, SetAuthControlReq.class));
            case MODIFY:
                return masterService.baseRequestMaster(gson.fromJson(req, ModifyTopicReq.class));
            case DELETE:
            case REMOVE:
                return masterService.baseRequestMaster(gson.fromJson(req, DeleteTopicReq.class));
            default:
                return TubeMQResult.getErrorResult("no such method");
        }
    }

    /**
     * query consumer auth control, shows all consumer groups
     * @param req
     * @return
     * @throws Exception
     */
    @GetMapping("/consumerAuth")
    public @ResponseBody String queryConsumerAuth(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterService.getQueryUrl(req);
        return queryMaster(url);
    }

    /**
     * query topic config info
     * @param req
     * @return
     * @throws Exception
     */
    @GetMapping("/topicConfig")
    public @ResponseBody String queryTopicConfig(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterService.getQueryUrl(req);
        return queryMaster(url);
    }

}
