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
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.controller.node.request.AddBrokersReq;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.repository.NodeRepository;
import org.apache.tubemq.manager.service.NodeService;
import org.apache.tubemq.manager.utils.MasterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

import static org.apache.tubemq.manager.controller.TubeMQResult.getErrorResult;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.SCHEMA;
import static org.apache.tubemq.manager.utils.ConvertUtils.convertReqToQueryStr;
import static org.apache.tubemq.manager.utils.MasterUtils.*;

@RestController
@RequestMapping(path = "/v1/node")
@Slf4j
public class NodeController {

    public static final String NO_SUCH_METHOD = "no such method";
    public static final String OP_QUERY = "op_query";
    public static final String ADMIN_QUERY_CLUSTER_INFO = "admin_query_cluster_info";
    private final Gson gson = new Gson();
    private static final CloseableHttpClient httpclient = HttpClients.createDefault();

    @Autowired
    NodeService nodeService;

    @Autowired
    NodeRepository nodeRepository;

    @Autowired
    MasterUtils masterUtil;

    @RequestMapping(value = "/query/clusterInfo", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody String queryInfo(@RequestParam String type, @RequestParam String method,
            @RequestParam(required = false) Integer clusterId) {
        if (method.equals(ADMIN_QUERY_CLUSTER_INFO) && type.equals(OP_QUERY)) {
            return nodeService.queryClusterInfo(clusterId);
        }
        return gson.toJson(getErrorResult(NO_SUCH_METHOD));
    }


    /**
     * query brokers' run status
     * this method supports batch operation
     */
    @RequestMapping(value = "/query/brokerStatus", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody String queryBrokerDetail(
            @RequestParam Map<String, String> queryBody) throws Exception {
        String url = masterUtil.getQueryUrl(queryBody);
        return queryMaster(url);
    }


    /**
     * query brokers' configuration
     * this method supports batch operation
     */
    @RequestMapping(value = "/query/brokerConfig", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody String queryBrokerConfig(
            @RequestParam Map<String, String> queryBody) throws Exception {
        String url = masterUtil.getQueryUrl(queryBody);
        return queryMaster(url);
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
            TubeMQResult result = addBrokersToCluster(req, masterEntry);
            return gson.toJson(result);
        } else {
            TubeMQResult result = new TubeMQResult();
            result.setErrCode(-1);
            result.setResult(false);
            result.setErrMsg("token is not correct");
            return gson.toJson(result);
        }

    }


    /**
     * online brokers in cluster
     * this method supports batch operation
     */
    @RequestMapping(value = "/online", method = RequestMethod.GET)
    public @ResponseBody String onlineBrokers(
            @RequestParam Map<String, String> queryBody) throws Exception {
        return gson.toJson(masterUtil.redirectToMaster(queryBody));
    }

    /**
     * reload brokers in cluster
     * this method supports batch operation
     */
    @RequestMapping(value = "/reload", method = RequestMethod.GET)
    public @ResponseBody String reloadBrokers(
            @RequestParam Map<String, String> queryBody) throws Exception {
        return gson.toJson(masterUtil.redirectToMaster(queryBody));
    }

    /**
     * delete brokers in cluster
     * this method supports batch operation
     */
    @RequestMapping(value = "/delete", method = RequestMethod.GET)
    public @ResponseBody String deleteBrokers(
            @RequestParam Map<String, String> queryBody) throws Exception {
        TubeMQResult result = masterUtil.redirectToMaster(queryBody);
        return gson.toJson(result);
    }

    /**
     * change brokers' read mode in cluster
     * this method supports batch operation
     */
    @RequestMapping(value = "/setRead", method = RequestMethod.GET)
    public @ResponseBody String setBrokersRead(
            @RequestParam Map<String, String> queryBody) throws Exception {
        TubeMQResult result = masterUtil.redirectToMaster(queryBody);
        return gson.toJson(result);
    }

    /**
     * change brokers' write mode in cluster
     * this method supports batch operation
     */
    @RequestMapping(value = "/setWrite", method = RequestMethod.GET)
    public @ResponseBody String setBrokersWrite(
            @RequestParam Map<String, String> queryBody) throws Exception {
        TubeMQResult result = masterUtil.redirectToMaster(queryBody);
        return gson.toJson(result);
    }



    private TubeMQResult addBrokersToCluster(AddBrokersReq req, NodeEntry masterEntry) throws Exception {
        String url = SCHEMA + masterEntry.getIp() + ":" + masterEntry.getWebPort()
                + "/" + TUBE_REQUEST_PATH + "?" + convertReqToQueryStr(req);
        TubeMQResult tubeMQResult = requestMaster(url);
        return tubeMQResult;
    }


}
