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

import static org.apache.tubemq.manager.service.TubeMQHttpConst.SCHEMA;
import static org.apache.tubemq.manager.service.MasterService.*;

import com.google.gson.Gson;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.repository.NodeRepository;
import org.apache.tubemq.manager.service.MasterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/v1/cluster")
@Slf4j
public class ClusterController {

    private final Gson gson = new Gson();

    @Autowired
    private NodeRepository nodeRepository;

    @Autowired
    private MasterService masterService;

    /**
     * query cluster info
     */
    @RequestMapping(value = "/query", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody String queryInfo(
            @RequestParam Map<String, String> queryBody) throws Exception {
        String url = masterService.getQueryUrl(queryBody);
        return queryMaster(url);
    }

    /**
     * modify cluster info, need to check token and
     * make sure user has authorization to modify it.
     */
    @RequestMapping(value = "/modify", method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody String modifyClusterInfo(
            @RequestBody Map<String, String> requestBody) throws Exception {
        String token = requestBody.get("confModAuthToken");
        log.info("token is {}, request body size is {}", token, requestBody.keySet());
        int clusterId = Integer.parseInt(requestBody.get("clusterId"));
        if (StringUtils.isNotBlank(token)) {
            requestBody.remove("clusterId");
            NodeEntry nodeEntry = nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(
                    clusterId);
            String url = SCHEMA + nodeEntry.getIp() + ":" + nodeEntry.getWebPort()
                    + "/" + TUBE_REQUEST_PATH + "?" + covertMapToQueryString(requestBody);
            return gson.toJson(requestMaster(url));
        } else {
            TubeMQResult result = new TubeMQResult();
            result.setErrCode(-1);
            result.setResult(false);
            result.setErrMsg("token is not correct");
            return gson.toJson(result);
        }
    }


}
