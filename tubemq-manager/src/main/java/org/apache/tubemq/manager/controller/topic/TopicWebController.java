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

import static org.apache.tubemq.manager.utils.MasterUtils.queryMaster;

import com.google.gson.Gson;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.controller.node.request.BatchAddTopicReq;
import org.apache.tubemq.manager.controller.node.request.CloneOffsetReq;
import org.apache.tubemq.manager.controller.node.request.CloneTopicReq;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.repository.NodeRepository;
import org.apache.tubemq.manager.repository.TopicRepository;
import org.apache.tubemq.manager.service.NodeService;
import org.apache.tubemq.manager.service.TopicBackendWorker;
import org.apache.tubemq.manager.utils.MasterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
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
    private TopicRepository topicRepository;

    @Autowired
    private TopicBackendWorker topicBackendWorker;

    @Autowired
    private NodeService nodeService;

    @Autowired
    private NodeRepository nodeRepository;

    public Gson gson = new Gson();

    @Autowired
    private MasterUtils masterUtils;

    /**
     * add topic to brokers
     * @param req
     * @return
     */
    @PostMapping("/add")
    public TubeMQResult addTopic(@RequestBody BatchAddTopicReq req) {
        if (req.getClusterId() == null)
            return TubeMQResult.getErrorResult("please input clusterId");
        NodeEntry masterEntry = nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(
            req.getClusterId());
        if (masterEntry == null)
            return TubeMQResult.getErrorResult("no such cluster");
        return nodeService.addTopicsToBrokers(masterEntry, req.getBrokerIds(), req.getAddTopicReqs());
    }

    /**
     * given one topic, copy its config and clone to brokers
     * if no broker is is provided, topics will be cloned to all brokers in cluster
     * @param req
     * @return
     * @throws Exception
     */
    @PostMapping("/clone")
    public TubeMQResult cloneTopic(@RequestBody CloneTopicReq req) throws Exception {
        if (req.getClusterId() == null)
            return TubeMQResult.getErrorResult("please input clusterId");
        NodeEntry masterEntry = nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(
            req.getClusterId());
        if (masterEntry == null)
            return TubeMQResult.getErrorResult("no such cluster");
        return nodeService.cloneTopicToBrokers(req, masterEntry);
    }

    /**
     * batch modify topic config
     * @param req
     * @return
     * @throws Exception
     */
    @PostMapping("/modify")
    public @ResponseBody String modifyTopics(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterUtils.getQueryUrl(req);
        return queryMaster(url);
    }

    /**
     * batch delete topic info
     * @param req
     * @return
     * @throws Exception
     */
    @PostMapping("/delete")
    public @ResponseBody String deleteTopics(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterUtils.getQueryUrl(req);
        return queryMaster(url);
    }


    /**
     * batch remove topics
     * @param req
     * @return
     * @throws Exception
     */
    @PostMapping("/remove")
    public @ResponseBody String removeTopics(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterUtils.getQueryUrl(req);
        return queryMaster(url);
    }

    /**
     * query consumer auth control, shows all consumer groups
     * @param req
     * @return
     * @throws Exception
     */
    @PostMapping("/query/consumer-auth")
    public @ResponseBody String queryConsumerAuth(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterUtils.getQueryUrl(req);
        return queryMaster(url);
    }

    /**
     * query topic config info
     * @param req
     * @return
     * @throws Exception
     */
    @PostMapping("/query/config")
    public @ResponseBody String queryTopicConfig(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterUtils.getQueryUrl(req);
        return queryMaster(url);
    }


    /**
     *
     * @param req
     * @return
     * @throws Exception
     */
    @PostMapping("/clone/offset")
    public @ResponseBody TubeMQResult cloneOffset(
        @RequestBody CloneOffsetReq req) throws Exception {
        if (req.getClusterId() == null)
            return TubeMQResult.getErrorResult("please input clusterId");
        NodeEntry masterEntry = nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(
            req.getClusterId());
        if (masterEntry == null)
            return TubeMQResult.getErrorResult("no such cluster");
        return nodeService.cloneOffsetToOtherGroups(req, masterEntry);
    }


}
