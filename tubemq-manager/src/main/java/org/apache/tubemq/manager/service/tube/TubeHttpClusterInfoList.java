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
package org.apache.tubemq.manager.service.tube;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.entry.NodeEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class TubeHttpClusterInfoList extends TubeMQResult {

    private List<ClusterData> clusterData = new ArrayList<>();

    @Data
    @AllArgsConstructor
    public static class ClusterData {

        @Data
        public static class ClusterInfo {

            @Data
            public static class BrokerInfo {
                private int brokerId;
                private String brokerIp;
            }

            private String master;
            private List<String> standby = new ArrayList<>();
            private List<BrokerInfo> broker = new ArrayList<>();

        }

        private int clusterId;
        private String clusterName;
        private ClusterInfo clusterInfo;

    }


    public static TubeHttpClusterInfoList getClusterInfoList(Map<Integer, List<NodeEntry>> nodeEntriesPerCluster) {
        // for each cluster provide cluster information
        TubeHttpClusterInfoList clusterInfoList = new TubeHttpClusterInfoList();
        nodeEntriesPerCluster.forEach((id, entries) -> {
                    ClusterData.ClusterInfo singleClusterInfo = getSingleClusterInfo(entries);
                    ClusterData clusterData =
                            new ClusterData(id, entries.get(0).getClusterName(), singleClusterInfo);
                    clusterInfoList.getClusterData().add(clusterData);
                }
        );
        return clusterInfoList;
    }

    private static ClusterData.ClusterInfo getSingleClusterInfo(List<NodeEntry> entries) {

        TubeHttpClusterInfoList.ClusterData.ClusterInfo clusterInfo =
                new TubeHttpClusterInfoList.ClusterData.ClusterInfo();

        entries.forEach(nodeEntry -> {
            if (nodeEntry.isMaster()) {
                clusterInfo.setMaster(nodeEntry.getIp());
            }
            if (nodeEntry.isBroker()) {
                ClusterData.ClusterInfo.BrokerInfo brokerInfo =
                        new ClusterData.ClusterInfo.BrokerInfo();
                brokerInfo.setBrokerId((int) nodeEntry.getBrokerId());
                brokerInfo.setBrokerIp(nodeEntry.getIp());
                clusterInfo.getBroker().add(brokerInfo);
            }
            if (nodeEntry.isStandby()) {
                clusterInfo.getStandby().add(nodeEntry.getIp());
            }
        });

        return clusterInfo;
    }


}
