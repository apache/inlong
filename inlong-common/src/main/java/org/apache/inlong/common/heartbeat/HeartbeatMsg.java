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

package org.apache.inlong.common.heartbeat;

import org.apache.inlong.common.enums.NodeSrvStatus;

import com.google.common.base.Joiner;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Heartbeat template for all components.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class HeartbeatMsg {

    /**
     * Node service status
     */
    private NodeSrvStatus nodeSrvStatus = NodeSrvStatus.OK;

    /**
     * Ip of component
     */
    private String ip;

    /**
     * Port of component
     */
    private String port;

    private List<AddressInfo> addressInfos;

    /**
     * ProtocolType of component
     */
    private String protocolType;

    /**
     * Type of component
     */
    private String componentType;

    /**
     * Type of report source
     */
    private String reportSourceType;

    /**
     * Report time millis of component
     */
    private Long reportTime;

    /**
     * Name of cluster
     */
    private String clusterName = "default";

    /**
     * Tag of cluster, separated by commas(,)
     */
    private String clusterTag;

    /**
      * Group of node for filtering stream source collect task, separated by commas(,)
      */
    private String nodeGroup;

    /**
     * Ext tag of cluster, key=value pairs seperated by &
     */
    private String extTag;

    /**
     * Name of responsible person, separated by commas(,)
     */
    private String inCharges = "admin";

    /**
     * Heartbeat msg of group if exists
     */
    private List<GroupHeartbeat> groupHeartbeats;

    /**
     * Heartbeat msg of stream if exists
     */
    private List<StreamHeartbeat> streamHeartbeats;

    /**
     * node load value
     */
    private Integer load = 0xffff;

    public ComponentHeartbeat componentHeartbeat() {
        if (addressInfos != null) {
            port = Joiner.on(",").join(addressInfos.stream().map(AddressInfo::getPort).collect(Collectors.toList()));
            ip = Joiner.on(",").join(addressInfos.stream().map(AddressInfo::getIp).collect(Collectors.toList()));
            reportSourceType = Joiner.on(",")
                    .join(addressInfos.stream().map(AddressInfo::getReportSourceType).collect(Collectors.toList()));
            protocolType = Joiner.on(",")
                    .join(addressInfos.stream().map(AddressInfo::getProtocolType).collect(Collectors.toList()));
        }
        return new ComponentHeartbeat(nodeSrvStatus, clusterTag, extTag, clusterName,
                componentType, ip, port, inCharges, protocolType, load);
    }
}
