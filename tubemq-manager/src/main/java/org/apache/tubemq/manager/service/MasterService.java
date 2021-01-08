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


package org.apache.tubemq.manager.service;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.controller.node.request.AddTopicReq;
import org.apache.tubemq.manager.controller.node.request.BaseReq;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.repository.NodeRepository;
import org.apache.tubemq.manager.service.tube.TubeHttpResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;

import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.tubemq.manager.controller.TubeMQResult.getErrorResult;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.SCHEMA;
import static org.apache.tubemq.manager.utils.ConvertUtils.convertReqToQueryStr;


@Slf4j
@Component
public class MasterService {

    public static final int SUCCESS_CODE = 0;
    private static CloseableHttpClient httpclient = HttpClients.createDefault();
    private static Gson gson = new Gson();
    public static final String TUBE_REQUEST_PATH = "webapi.htm";

    @Autowired
    NodeRepository nodeRepository;

    public static String covertMapToQueryString(Map<String, String> requestMap) throws Exception {
        List<String> queryList = new ArrayList<>();

        for (Map.Entry<String, String> entry : requestMap.entrySet()) {
            queryList.add(entry.getKey() + "=" + URLEncoder.encode(
                    entry.getValue(), UTF_8.toString()));
        }
        return StringUtils.join(queryList, "&");
    }



    public static TubeMQResult requestMaster(String url) {

        log.info("start to request {}", url);
        HttpGet httpGet = new HttpGet(url);
        TubeMQResult defaultResult = new TubeMQResult();

        try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
            TubeHttpResponse tubeResponse =
                    gson.fromJson(new InputStreamReader(response.getEntity().getContent()),
                            TubeHttpResponse.class);
            if (tubeResponse.getCode() == SUCCESS_CODE && tubeResponse.getErrCode() == SUCCESS_CODE) {
                return defaultResult;
            } else {
                defaultResult = getErrorResult(tubeResponse.getErrMsg());
            }
        } catch (Exception ex) {
            log.error("exception caught while requesting broker status", ex);
            defaultResult = getErrorResult(ex.getMessage());
        }
        return defaultResult;
    }

    /**
     * query master to get node info
     * @param url
     * @return query info
     */
    public static String queryMaster(String url) {
        log.info("start to request {}", url);
        HttpGet httpGet = new HttpGet(url);
        TubeMQResult defaultResult = new TubeMQResult();
        try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
            // return result json to response
            return EntityUtils.toString(response.getEntity());
        } catch (Exception ex) {
            log.error("exception caught while requesting broker status", ex);
            defaultResult.setErrCode(-1);
            defaultResult.setResult(false);
            defaultResult.setErrMsg(ex.getMessage());
        }
        return gson.toJson(defaultResult);
    }


    public TubeMQResult baseRequestMaster(BaseReq req) {
        if (req.getClusterId() == null) {
            return TubeMQResult.getErrorResult("please input clusterId");
        }
        NodeEntry masterEntry = nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(
            req.getClusterId());
        if (masterEntry == null) {
            return TubeMQResult.getErrorResult("no such cluster");
        }
        String url = SCHEMA + masterEntry.getIp() + ":" + masterEntry.getWebPort()
            + "/" + TUBE_REQUEST_PATH + "?" + convertReqToQueryStr(req);
        return requestMaster(url);
    }


    public NodeEntry getMasterNode(BaseReq req) {
        if (req.getClusterId() == null) {
            return null;
        }
        return nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(
            req.getClusterId());
    }


    public String getQueryUrl(Map<String, String> queryBody) throws Exception {
        int clusterId = Integer.parseInt(queryBody.get("clusterId"));
        queryBody.remove("clusterId");
        NodeEntry nodeEntry =
                nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(clusterId);
        return SCHEMA + nodeEntry.getIp() + ":" + nodeEntry.getWebPort()
                + "/" + TUBE_REQUEST_PATH + "?" + covertMapToQueryString(queryBody);
    }

}
