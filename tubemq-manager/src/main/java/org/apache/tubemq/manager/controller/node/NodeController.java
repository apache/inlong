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
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.service.NodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(path = "/v1/node")
@Slf4j
public class NodeController {

    public static final String NO_SUCH_METHOD = "no such method";
    public static final String OP_QUERY = "op_query";
    public static final String ADMIN_QUERY_CLUSTER_INFO = "admin_query_cluster_info";

    private final Gson gson = new Gson();

    @Autowired
    NodeService nodeService;

    @RequestMapping(value = "/query", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody String queryInfo(@RequestParam String type, @RequestParam String method,
            @RequestParam(required = false) Integer clusterId) {

        if (method.equals(ADMIN_QUERY_CLUSTER_INFO) && type.equals(OP_QUERY)) {
            return nodeService.queryClusterInfo(clusterId);
        }

        return gson.toJson(TubeMQResult.getErrorResult(NO_SUCH_METHOD));
    }

}
