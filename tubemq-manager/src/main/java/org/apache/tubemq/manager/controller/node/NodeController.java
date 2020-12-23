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

package org.apache.tubemq.manager.controller.node;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.controller.node.request.AddBrokersReq;
import org.apache.tubemq.manager.controller.node.request.BrokerConf;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.repository.NodeRepository;
import org.apache.tubemq.manager.service.NodeService;
import org.apache.tubemq.manager.service.tube.TubeHttpResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tubemq.manager.service.TubeMQHttpConst.SCHEMA;
import static org.apache.tubemq.manager.utils.ConvertUtils.convertReqToQueryStr;

@RestController
@RequestMapping(path = "/v1/node")
@Slf4j
public class NodeController {

    public static final String NO_SUCH_METHOD = "no such method";
    public static final String OP_QUERY = "op_query";
    public static final String ADMIN_QUERY_CLUSTER_INFO = "admin_query_cluster_info";
    public static final String TUBE_REQUEST_PATH = "webapi.htm";
    private final Gson gson = new Gson();
    private static final CloseableHttpClient httpclient = HttpClients.createDefault();

    @Autowired
    NodeService nodeService;

    @Autowired
    NodeRepository nodeRepository;


    @RequestMapping(value = "/query", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody String queryInfo(@RequestParam String type, @RequestParam String method,
            @RequestParam(required = false) Integer clusterId) {

        if (method.equals(ADMIN_QUERY_CLUSTER_INFO) && type.equals(OP_QUERY)) {
            return nodeService.queryClusterInfo(clusterId);
        }

        return gson.toJson(TubeMQResult.getErrorResult(NO_SUCH_METHOD));
    }


    /**
     * add brokers to cluster, need to check token and
     * make sure user has authorization to modify it.
     */
    @RequestMapping(value = "/add", method = RequestMethod.POST)
    public @ResponseBody String addBrokersToCluster(
            @RequestBody AddBrokersReq req) throws Exception {
        String token = req.getConfModAuthToken();
        int clusterId = req.getClusterId();

        if (StringUtils.isNotBlank(token)) {
            NodeEntry masterEntry = nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(
                    clusterId);
            return addToMasterAndRepo(req, masterEntry);
        } else {
            TubeMQResult result = new TubeMQResult();
            result.setErrCode(-1);
            result.setResult(false);
            result.setErrMsg("token is not correct");
            return gson.toJson(result);
        }

    }

    private String addToMasterAndRepo(AddBrokersReq req, NodeEntry masterEntry) throws Exception {

        String url = SCHEMA + masterEntry.getIp() + ":" + masterEntry.getWebPort()
                + "/" + TUBE_REQUEST_PATH + "?" + convertReqToQueryStr(req);

        log.info("start to request {}", url);
        HttpGet httpGet = new HttpGet(url);
        TubeMQResult defaultResult = new TubeMQResult();

        try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
            TubeHttpResponse result =
                    gson.fromJson(new InputStreamReader(response.getEntity().getContent()),
                            TubeHttpResponse.class);
            if (result.getCode() == 0 && result.getErrCode() == 0) {
                // save brokers to db when success
                saveAllBrokers(req.getBrokerJsonSet(), req.getClusterId(), masterEntry);
            } else {
                return result.getErrMsg();
            }
        } catch (Exception ex) {
            log.error("exception caught while requesting broker status", ex);
            defaultResult.setErrCode(-1);
            defaultResult.setResult(false);
            defaultResult.setErrMsg(ex.getMessage());
        }

        return gson.toJson(defaultResult);
    }




    private void saveAllBrokers(List<BrokerConf> brokerConfList, int clusterId, NodeEntry masterEntry) {
        List<NodeEntry> nodeEntries = new ArrayList<>();
        for (BrokerConf brokerConf : brokerConfList) {
            NodeEntry node = new NodeEntry();
            node.setBroker(true);
            node.setClusterId(clusterId);
            node.setClusterName(masterEntry.getClusterName());
            node.setBrokerId(brokerConf.getBrokerId());
            node.setMaster(false);
            node.setIp(brokerConf.getBrokerIp());
            node.setStandby(false);
            node.setPort(brokerConf.getBrokerPort());
            nodeEntries.add(node);
        }
        nodeService.saveNodes(nodeEntries);
    }

}
