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

package org.apache.tubemq.manager.controller.cluster;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.tubemq.manager.service.TubeHttpConst.SCHEMA;

import com.google.gson.Gson;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tubemq.manager.controller.TubeResult;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.repository.NodeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/v1/cluster")
@Slf4j
public class ClusterController {

    private final CloseableHttpClient httpclient = HttpClients.createDefault();
    private final Gson gson = new Gson();

    private static final String TUBE_REQUEST_PATH = "webapi.htm";

    @Autowired
    private NodeRepository nodeRepository;


    private String covertMapToQueryString(Map<String, String> requestMap) throws Exception {
        List<String> queryList = new ArrayList<>();

        for (Map.Entry<String, String> entry : requestMap.entrySet()) {
            queryList.add(entry.getKey() + "=" + URLEncoder.encode(
                    entry.getValue(), UTF_8.toString()));
        }
        return StringUtils.join(queryList, "&");
    }

    private String queryMaster(String url) {
        log.info("start to request {}", url);
        HttpGet httpGet = new HttpGet(url);
        TubeResult defaultResult = new TubeResult();
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

    @RequestMapping(value = "/query", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody String queryInfo(
            @RequestParam Map<String, String> queryBody) throws Exception {
        int clusterId = Integer.parseInt(queryBody.get("clusterId"));
        queryBody.remove("clusterId");
        NodeEntry nodeEntry =
                nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(clusterId);
        String url = SCHEMA + nodeEntry.getIp() + ":" + nodeEntry.getWebPort()
                + "/" + TUBE_REQUEST_PATH + "?" + covertMapToQueryString(queryBody);
        return queryMaster(url);
    }


}
