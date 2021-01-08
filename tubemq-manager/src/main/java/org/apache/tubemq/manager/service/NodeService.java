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

package org.apache.tubemq.manager.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.controller.node.request.AddBrokersReq;
import org.apache.tubemq.manager.controller.node.request.AddTopicReq;
import org.apache.tubemq.manager.controller.node.request.BatchAddTopicReq;
import org.apache.tubemq.manager.controller.node.request.CloneBrokersReq;
import org.apache.tubemq.manager.controller.node.request.CloneTopicReq;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.service.tube.AddBrokerResult;

public interface NodeService {

    /**
     * clone brokers with topic in it
     * @param req
     * @return
     * @throws Exception
     */
    TubeMQResult cloneBrokersWithTopic(CloneBrokersReq req) throws Exception;

    /**
     * add topics to brokers
     * @param masterEntry
     * @param brokerIds
     * @param addTopicReqs
     * @return
     */
    TubeMQResult addTopicsToBrokers(NodeEntry masterEntry, List<Integer> brokerIds,
        List<AddTopicReq> addTopicReqs);

    /**
     * add one topic to brokers
     * @param req
     * @param masterEntry
     * @return
     * @throws Exception
     */
    TubeMQResult addTopicToBrokers(AddTopicReq req, NodeEntry masterEntry) throws Exception;

    /**
     * update broker status
     * @param clusterId
     * @param pendingTopic
     */
    void updateBrokerStatus(int clusterId, Map<String, TopicFuture> pendingTopic);

    /**
     * query cluster info
     * @param clusterId
     * @return
     */
    String queryClusterInfo(Integer clusterId);

    void close() throws IOException;

    /**
     * clone topic to brokers
     * @param req
     * @return
     * @throws Exception
     */
    TubeMQResult cloneTopicToBrokers(CloneTopicReq req) throws Exception;

    /**
     * batch add topic to master
     * @param req
     * @return
     */
    TubeMQResult batchAddTopic(BatchAddTopicReq req);
}
