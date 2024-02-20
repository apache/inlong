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

package org.apache.inlong.tubemq.manager.controller.topic;

import org.apache.inlong.tubemq.manager.controller.TubeMQResult;
import org.apache.inlong.tubemq.manager.controller.node.request.BatchAddTopicReq;
import org.apache.inlong.tubemq.manager.controller.node.request.CloneTopicReq;
import org.apache.inlong.tubemq.manager.controller.topic.request.DeleteTopicReq;
import org.apache.inlong.tubemq.manager.controller.topic.request.ModifyTopicReq;
import org.apache.inlong.tubemq.manager.controller.topic.request.QueryCanWriteReq;
import org.apache.inlong.tubemq.manager.controller.topic.request.SetAuthControlReq;
import org.apache.inlong.tubemq.manager.controller.topic.request.SetPublishReq;
import org.apache.inlong.tubemq.manager.controller.topic.request.SetSubscribeReq;
import org.apache.inlong.tubemq.manager.service.TubeConst;
import org.apache.inlong.tubemq.manager.service.TubeMQErrorConst;
import org.apache.inlong.tubemq.manager.service.interfaces.MasterService;
import org.apache.inlong.tubemq.manager.service.interfaces.NodeService;
import org.apache.inlong.tubemq.manager.service.interfaces.TopicService;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(path = "/v1/topic")
@Slf4j
public class TopicWebController {

    private static final Logger LOGGER = LogManager.getLogger(TopicWebController.class);

    @Autowired
    private NodeService nodeService;

    private static final Gson gson = new Gson();

    @Autowired
    private MasterService masterService;

    @Autowired
    private TopicService topicService;

    /**
     * Broker method proxy.
     * Divides the operation on the broker into different methods.
     */
    @RequestMapping(value = "")
    public @ResponseBody TubeMQResult topicMethodProxy(@RequestParam String method, @RequestBody String req)
            throws Exception {
        // Log audit: Record the received method and req parameters
        LOGGER.info("Received method for topicMethodProxy: {}", method);
        LOGGER.info("Received req for topicMethodProxy: {}", req);

        // Validate the 'method' parameter
        if (!isValidMethod(method)) {
            return handleInvalidMethod(method);
        }

        // Validate the 'req' parameter
        if (!isValidJson(req)) {
            return handleInvalidJson(req);
        }

        // Perform processing based on the 'method' parameter
        switch (method) {
            case TubeConst.ADD:
                return handleAddTopicRequest(req);
            case TubeConst.CLONE:
                return handleCloneTopicRequest(req);
            case TubeConst.AUTH_CONTROL:
                return handleAuthControlRequest(req);
            case TubeConst.MODIFY:
                return handleModifyTopicRequest(req);
            case TubeConst.DELETE:
            case TubeConst.REMOVE:
                return handleDeleteTopicRequest(req);
            case TubeConst.QUERY_CAN_WRITE:
                return handleQueryCanWriteRequest(req);
            case TubeConst.PUBLISH:
                return handlePublishRequest(req);
            case TubeConst.SUBSCRIBE:
                return handleSubscribeRequest(req);
            default:
                return handleInvalidMethod(method);
        }
    }

    /**
     * Handles invalid 'method' parameter.
     *
     * @param method The invalid method value
     * @return TubeMQResult indicating an error
     */
    private TubeMQResult handleInvalidMethod(String method) {
        LOGGER.warn("Invalid method value received: {}", method);
        return TubeMQResult.errorResult("Invalid method value.");
    }

    /**
     * Handles invalid JSON format in 'req' parameter.
     *
     * @param req The invalid JSON format
     * @return TubeMQResult indicating an error
     */
    private TubeMQResult handleInvalidJson(String req) {
        LOGGER.warn("Invalid JSON format received: {}", req);
        return TubeMQResult.errorResult("Invalid JSON format.");
    }

    /**
     * Handles 'ADD' method request.
     *
     * @param req The JSON request
     * @return TubeMQResult based on the request
     */
    private TubeMQResult handleAddTopicRequest(String req) {
        return masterService.baseRequestMaster(gson.fromJson(req, BatchAddTopicReq.class));
    }

    /**
     * Handles 'CLONE' method request.
     *
     * @param req The JSON request
     * @return TubeMQResult based on the request
     */
    private TubeMQResult handleCloneTopicRequest(String req) throws Exception {
        return nodeService.cloneTopicToBrokers(gson.fromJson(req, CloneTopicReq.class));
    }

    /**
     * Handles 'AUTH_CONTROL' method request.
     *
     * @param req The JSON request
     * @return TubeMQResult based on the request
     */
    private TubeMQResult handleAuthControlRequest(String req) {
        SetAuthControlReq setAuthControlReq = gson.fromJson(req, SetAuthControlReq.class);
        setAuthControlReq.setMethod(TubeConst.SET_AUTH_CONTROL);
        setAuthControlReq.setType(TubeConst.OP_MODIFY);
        setAuthControlReq.setCreateUser(TubeConst.TUBEADMIN);
        return masterService.baseRequestMaster(setAuthControlReq);
    }

    /**
     * Handles 'MODIFY' method request.
     *
     * @param req The JSON request
     * @return TubeMQResult based on the request
     */
    private TubeMQResult handleModifyTopicRequest(String req) {
        return masterService.baseRequestMaster(gson.fromJson(req, ModifyTopicReq.class));
    }

    /**
     * Handles 'DELETE' and 'REMOVE' method requests.
     *
     * @param req The JSON request
     * @return TubeMQResult based on the request
     */
    private TubeMQResult handleDeleteTopicRequest(String req) {
        return masterService.baseRequestMaster(gson.fromJson(req, DeleteTopicReq.class));
    }

    /**
     * Handles 'QUERY_CAN_WRITE' method request.
     *
     * @param req The JSON request
     * @return TubeMQResult based on the request
     */
    private TubeMQResult handleQueryCanWriteRequest(String req) {
        QueryCanWriteReq queryCanWriteReq = gson.fromJson(req, QueryCanWriteReq.class);
        if (!queryCanWriteReq.legal()) {
            return TubeMQResult.errorResult(TubeMQErrorConst.PARAM_ILLEGAL);
        }
        return topicService.queryCanWrite(queryCanWriteReq.getTopicName(), queryCanWriteReq.getClusterId());
    }

    /**
     * Handles 'PUBLISH' method request.
     *
     * @param req The JSON request
     * @return TubeMQResult based on the request
     */
    private TubeMQResult handlePublishRequest(String req) {
        return masterService.baseRequestMaster(gson.fromJson(req, SetPublishReq.class));
    }

    /**
     * Handles 'SUBSCRIBE' method request.
     *
     * @param req The JSON request
     * @return TubeMQResult based on the request
     */
    private TubeMQResult handleSubscribeRequest(String req) {
        return masterService.baseRequestMaster(gson.fromJson(req, SetSubscribeReq.class));
    }

    /**
     * Checks if the given method is valid by comparing it against a list of allowed methods.
     *
     * @param method The method to validate.
     * @return {@code true} if the method is valid, {@code false} otherwise.
     */
    private static boolean isValidMethod(String method) {
        // Define a list of allowed methods
        List<String> allowedMethods = Arrays.asList(
                TubeConst.ADD, TubeConst.CLONE, TubeConst.AUTH_CONTROL, TubeConst.MODIFY,
                TubeConst.DELETE, TubeConst.REMOVE, TubeConst.QUERY_CAN_WRITE, TubeConst.PUBLISH, TubeConst.SUBSCRIBE);
        return allowedMethods.contains(method);
    }

    /**
     * Validates whether the given JSON string has a valid format by attempting to parse it.
     *
     * @param json The JSON string to validate.
     * @return {@code true} if the JSON format is valid, {@code false} otherwise.
     */
    private static boolean isValidJson(String json) {
        // Use a JSON library or parser to validate the JSON format
        try {
            gson.fromJson(json, Object.class);
            return true;
        } catch (JsonSyntaxException e) {
            LOGGER.error("JSON validation failed with exception: {}", e.getMessage());
            return false;
        }
    }

    /**
     * query consumer auth control, shows all consumer groups
     *
     * @param req
     * @return
     *
     * @throws Exception the exception
     */
    @GetMapping("/consumerAuth")
    public @ResponseBody String queryConsumerAuth(
            @RequestParam Map<String, String> req) throws Exception {
        String url = masterService.getQueryUrl(req);
        return masterService.queryMaster(url);
    }

    /**
     * query topic config info
     *
     * @param req
     * @return
     *
     * @throws Exception the exception
     */
    @GetMapping("/topicConfig")
    public @ResponseBody String queryTopicConfig(
            @RequestParam Map<String, String> req) throws Exception {
        String url = masterService.getQueryUrl(req);
        return masterService.queryMaster(url);
    }

}
