/**
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

package org.apache.tubemq.manager.controller.group;


import static org.apache.tubemq.manager.controller.node.NodeController.ADD;
import static org.apache.tubemq.manager.controller.node.NodeController.CLONE;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.SCHEMA;
import static org.apache.tubemq.manager.utils.MasterUtils.TUBE_REQUEST_PATH;
import static org.apache.tubemq.manager.utils.MasterUtils.queryMaster;
import static org.apache.tubemq.manager.utils.MasterUtils.requestMaster;

import com.google.gson.Gson;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.controller.group.request.DeleteOffsetReq;
import org.apache.tubemq.manager.controller.node.request.CloneOffsetReq;
import org.apache.tubemq.manager.controller.topic.request.BatchAddGroupAuthReq;
import org.apache.tubemq.manager.controller.topic.request.DeleteGroupReq;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.repository.NodeRepository;
import org.apache.tubemq.manager.service.NodeService;
import org.apache.tubemq.manager.service.TopicService;
import org.apache.tubemq.manager.utils.ConvertUtils;
import org.apache.tubemq.manager.utils.MasterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/v1/group")
@Slf4j
public class GroupController {

    public static final String DELETE = "delete";

    public Gson gson = new Gson();

    @Autowired
    private MasterUtils masterUtils;

    @Autowired
    private TopicService topicService;


    @PostMapping("/")
    public @ResponseBody TubeMQResult groupMethodProxy(
        @RequestParam String method, @RequestBody String req) throws Exception {
        switch (method) {
            case ADD:
                return topicService.addConsumer(gson.fromJson(req, BatchAddGroupAuthReq.class));
            case DELETE:
                return topicService.deleteConsumer(gson.fromJson(req, DeleteGroupReq.class));
            default:
                return TubeMQResult.getErrorResult("no such method");
        }
    }

    /**
     * query the consumer group for certain topic
     * @param req
     * @return
     * @throws Exception
     */
    @GetMapping("/")
    public @ResponseBody String queryConsumer(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterUtils.getQueryUrl(req);
        return queryMaster(url);
    }


    @PostMapping("/offset")
    public @ResponseBody TubeMQResult offsetMethodProxy(
        @RequestParam String method, @RequestBody String req) {
        switch (method) {
            case CLONE:
                return topicService.cloneOffset(gson.fromJson(req, CloneOffsetReq.class));
            case DELETE:
                return topicService.deleteOffset(gson.fromJson(req, DeleteOffsetReq.class));
            default:
                return TubeMQResult.getErrorResult("no such method");
        }

    }

    /**
     * add group to black list for certain topic
     * @param req
     * @return
     * @throws Exception
     */
    @PostMapping("/blackGroup/add")
    public @ResponseBody
    TubeMQResult addBlackGroup(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterUtils.getQueryUrl(req);
        return requestMaster(url);
    }

    /**
     * delete group to black list for certain topic
     * @param req
     * @return
     * @throws Exception
     */
    @GetMapping("/blackGroup/delete")
    public @ResponseBody TubeMQResult deleteBlackGroup(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterUtils.getQueryUrl(req);
        return requestMaster(url);
    }

    /**
     * query the black list for certain topic
     * @param req
     * @return
     * @throws Exception
     */
    @GetMapping("/blackGroup/query")
    public @ResponseBody String queryBlackGroup(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterUtils.getQueryUrl(req);
        return queryMaster(url);
    }


}
