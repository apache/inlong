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


package org.apache.tubemq.manager.service.interfaces;

import java.util.Map;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.controller.node.request.BaseReq;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.springframework.stereotype.Component;

@Component
public interface MasterService {

    /**
     * request master with request url, return action result (success or fail)
     * @param url
     * @return
     */
    TubeMQResult requestMaster(String url);

    /**
     * query master with query url, return the information returned by master
     * @param url
     * @return
     */
    String queryMaster(String url);

    /**
     * request master with baseReq, return action result (success or fail)
     * @param req
     * @return
     */
    TubeMQResult baseRequestMaster(BaseReq req);

    /**
     * get the master node in the cluster
     * @param req
     * @return
     */
    NodeEntry getMasterNode(BaseReq req);

    /**
     * use queryBody to generate queryUrl for master query
     * @param queryBody
     * @return
     * @throws Exception
     */
    String getQueryUrl(Map<String, String> queryBody) throws Exception;

    /**
     * check whether the master node is alive
     * @param masterIp
     * @param masterPort
     * @return
     */
    TubeMQResult checkMasterNodeStatus(String masterIp, Integer masterPort);
}
