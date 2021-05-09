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

package org.apache.tubemq.server.master.nodemanage.nodebroker;

import java.util.List;
import java.util.Map;
import org.apache.tubemq.corebase.cluster.TopicInfo;
import org.apache.tubemq.corebase.utils.Tuple3;
import org.apache.tubemq.server.common.statusdef.ManageStatus;
import org.apache.tubemq.server.common.utils.ProcessResult;


public interface BrokerRunManager {

    boolean brokerRegister2M(String clientId, boolean enableTls,
                             int tlsPort, long reportConfigId,
                             int reportCheckSumId, boolean isTackData,
                             String repBrokerConfInfo,
                             List<String> repTopicConfInfo,
                             boolean isOnline, boolean isOverTLS,
                             StringBuilder sBuffer, ProcessResult result);

    boolean brokerHeartBeat2M(int brokerId, long reportConfigId,
                              int reportCheckSumId, boolean isTackData,
                              String repBrokerConfInfo,
                              List<String> repTopicConfInfo, boolean isOnline,
                              StringBuilder sBuffer, ProcessResult result);

    boolean brokerClose2M(int brokerId);

    boolean brokerTimeout(int brokerId, long bookedId);


    Tuple3<ManageStatus, String, Map<String, String>> getBrokerMetaConfigInfo(int brokerId);

    void updBrokerCsmConfInfo(int brokerId,
                              ManageStatus mngStatus,
                              Map<String, TopicInfo> topicInfoMap);

    void updBrokerPrdConfInfo(int brokerId,
                              ManageStatus mngStatus,
                              Map<String, TopicInfo> topicInfoMap);

}
