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

package org.apache.tubemq.server.master.balance;

import java.util.List;
import java.util.Map;
import org.apache.tubemq.corebase.cluster.ConsumerInfo;
import org.apache.tubemq.corebase.cluster.Partition;
import org.apache.tubemq.server.common.offsetstorage.OffsetStorage;
import org.apache.tubemq.server.master.nodemanage.nodebroker.BrokerConfManage;
import org.apache.tubemq.server.master.nodemanage.nodebroker.BrokerInfoHolder;
import org.apache.tubemq.server.master.nodemanage.nodebroker.TopicPSInfoManager;
import org.apache.tubemq.server.master.nodemanage.nodeconsumer.ConsumerInfoHolder;


public interface LoadBalancer {

    Map<String, Map<String, List<Partition>>> balanceCluster(
            Map<String, Map<String, Map<String, Partition>>> clusterState,
            ConsumerInfoHolder consumerHolder,
            BrokerInfoHolder brokerHolder,
            TopicPSInfoManager topicPSInfoManager,
            List<String> groups,
            BrokerConfManage brokerConfManage,
            int defAllowBClientRate,
            final StringBuilder sBuilder);

    Map<String, Map<String, Map<String, Partition>>> resetBalanceCluster(
            Map<String, Map<String, Map<String, Partition>>> clusterState,
            ConsumerInfoHolder consumerHolder,
            TopicPSInfoManager topicPSInfoManager,
            List<String> groups,
            OffsetStorage zkOffsetStorage,
            BrokerConfManage defaultBrokerConfManage,
            final StringBuilder sBuilder);

    Map<String, Map<String, List<Partition>>> bukAssign(ConsumerInfoHolder consumerHolder,
                                                        TopicPSInfoManager topicPSInfoManager,
                                                        List<String> groups,
                                                        BrokerConfManage brokerConfManage,
                                                        int defAllowBClientRate,
                                                        final StringBuilder sBuilder);

    Map<String, Map<String, Map<String, Partition>>> resetBukAssign(ConsumerInfoHolder consumerHolder,
                                                                    TopicPSInfoManager topicPSInfoManager,
                                                                    List<String> groups,
                                                                    OffsetStorage zkOffsetStorage,
                                                                    BrokerConfManage defaultBrokerConfManage,
                                                                    final StringBuilder sBuilder);

    Map<String, List<Partition>> roundRobinAssignment(List<Partition> partitions,
                                                      List<String> consumers);


    ConsumerInfo randomAssignment(List<ConsumerInfo> servers);
}
