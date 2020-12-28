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

package org.apache.tubemq.manager.controller.node.request;

import lombok.Data;

import static org.apache.tubemq.manager.service.TubeMQHttpConst.OP_QUERY;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.QUERY_BROKER_CONFIG;

@Data
public class QueryBrokerCfgReq {

    public String method;

    public Integer brokerId;

    public String type;

    public boolean withDetail;

    public boolean withTopic;

    public static QueryBrokerCfgReq getReq(Integer brokerId) {
        QueryBrokerCfgReq req = new QueryBrokerCfgReq();
        req.setBrokerId(brokerId);
        req.setMethod(QUERY_BROKER_CONFIG);
        req.setType(OP_QUERY);
        req.setWithTopic(false);
        req.setWithDetail(true);
        return req;
    }
}
