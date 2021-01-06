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

import static org.apache.tubemq.manager.service.TubeMQHttpConst.SCHEMA;
import static org.apache.tubemq.manager.utils.MasterUtils.TUBE_REQUEST_PATH;
import static org.apache.tubemq.manager.utils.MasterUtils.queryMaster;
import static org.apache.tubemq.manager.utils.MasterUtils.requestMaster;

import com.google.gson.Gson;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.controller.node.request.BatchAddTopicReq;
import org.apache.tubemq.manager.controller.node.request.CloneOffsetReq;
import org.apache.tubemq.manager.controller.node.request.CloneTopicReq;
import org.apache.tubemq.manager.controller.topic.request.BatchAddGroupAuthReq;
import org.apache.tubemq.manager.controller.topic.request.DeleteGroupReq;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.repository.NodeRepository;
import org.apache.tubemq.manager.repository.TopicRepository;
import org.apache.tubemq.manager.service.NodeService;
import org.apache.tubemq.manager.service.TopicBackendWorker;
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
        if (req.getClusterId() == null) {
            return TubeMQResult.getErrorResult("please input clusterId");
        }
        NodeEntry masterEntry = nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(
            req.getClusterId());
        if (masterEntry == null) {
            return TubeMQResult.getErrorResult("no such cluster");
        }
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
        if (req.getClusterId() == null) {
            return TubeMQResult.getErrorResult("please input clusterId");
        }
        NodeEntry masterEntry = nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(
            req.getClusterId());
        if (masterEntry == null) {
            return TubeMQResult.getErrorResult("no such cluster");
        }
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
    @PostMapping("/query/topic-config")
    public @ResponseBody String queryTopicConfig(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterUtils.getQueryUrl(req);
        return queryMaster(url);
    }

    /**
     * add group to black list for certain topic
     * @param req
     * @return
     * @throws Exception
     */
    @GetMapping("/add/blackGroup")
    public @ResponseBody TubeMQResult addBlackGroup(
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
    @GetMapping("/delete/blackGroup")
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
    @GetMapping("/query/blackGroup")
    public @ResponseBody String queryBlackGroup(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterUtils.getQueryUrl(req);
        return queryMaster(url);
    }

    /**
     * batch add consumer group for certain topic
     * @param req
     * @return
     * @throws Exception
     */
    @PostMapping("/add/group")
    public @ResponseBody TubeMQResult addConsumer(
        @RequestBody BatchAddGroupAuthReq req) throws Exception {
        NodeEntry nodeEntry =
            nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(req.getClusterId());
        if (nodeEntry == null) {
            return TubeMQResult.getErrorResult("no such cluster");
        }
        String url = SCHEMA + nodeEntry.getIp() + ":" + nodeEntry.getWebPort()
            + "/" + TUBE_REQUEST_PATH + "?" + ConvertUtils.convertReqToQueryStr(req);
        return requestMaster(url);
    }


    /**
     * delete consumer group for certain topic
     * @param req
     * @return
     * @throws Exception
     */
    @PostMapping("/delete/group")
    public @ResponseBody TubeMQResult deleteConsumer(
        @RequestBody DeleteGroupReq req) throws Exception {
        NodeEntry nodeEntry =
            nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(req.getClusterId());
        if (nodeEntry == null) {
            return TubeMQResult.getErrorResult("no such cluster");
        }
        String url = SCHEMA + nodeEntry.getIp() + ":" + nodeEntry.getWebPort()
            + "/" + TUBE_REQUEST_PATH + "?" + ConvertUtils.convertReqToQueryStr(req);
        return requestMaster(url);
    }

    /**
     * enable auth control for topics
     * @param req
     * @return
     * @throws Exception
     */
    @GetMapping("/enable/auth-control")
    public @ResponseBody TubeMQResult enableAuthControl(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterUtils.getQueryUrl(req);
        return requestMaster(url);
    }

    /**
     * disable auth control for topics
     * @param req
     * @return
     * @throws Exception
     */
    @GetMapping("/disable/auth-control")
    public @ResponseBody TubeMQResult disableAuthControl(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterUtils.getQueryUrl(req);
        return requestMaster(url);
    }

    /**
     * query the consumer group for certain topic
     * @param req
     * @return
     * @throws Exception
     */
    @GetMapping("/query/group")
    public @ResponseBody String queryConsumer(
        @RequestParam Map<String, String> req) throws Exception {
        String url = masterUtils.getQueryUrl(req);
        return queryMaster(url);
    }


    /**
     * clone offset from one group to another
     * @param req
     * @return
     * @throws Exception
     */
    @PostMapping("/clone/offset")
    public @ResponseBody TubeMQResult cloneOffset(
        @RequestBody CloneOffsetReq req) throws Exception {
        if (req.getClusterId() == null) {
            return TubeMQResult.getErrorResult("please input clusterId");
        }
        NodeEntry masterEntry = nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(
            req.getClusterId());
        if (masterEntry == null) {
            return TubeMQResult.getErrorResult("no such cluster");
        }
        return nodeService.cloneOffsetToOtherGroups(req, masterEntry);
    }


}
