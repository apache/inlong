/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.client.producer.qltystats;

import org.apache.inlong.tubemq.client.exception.TubeClientException;
import org.apache.inlong.tubemq.corebase.cluster.Partition;

import java.util.List;
import java.util.Map;

public interface BrokerRcvQltyStats {

    List<Partition> getAllowedBrokerPartitions(
            Map<Integer, List<Partition>> brokerPartList) throws TubeClientException;

    void statisticDltBrokerStatus();

    void addSendStatistic(int brokerId);

    void addReceiveStatistic(int brokerId, boolean isSuccess);

    void removeUnRegisteredBroker(List<Integer> registeredBrokerIdList);

    void stopBrokerStatistic();
}
