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

package org.apache.inlong.sdk.dataproxy.config;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Map;

public class ProxyConfigEntry implements java.io.Serializable {

    private int clusterId;
    private String groupId;
    private Map<String, HostInfo> hostMap;
    private int load;
    private int switchStat;
    private boolean isInterVisit;
    private int maxPacketLength;

    public int getMaxPacketLength() {
        return maxPacketLength;
    }

    public void setMaxPacketLength(int maxPacketLength) {
        this.maxPacketLength = maxPacketLength;
    }

    public int getLoad() {
        return load;
    }

    public void setLoad(int load) {
        this.load = load;
    }

    public int getSwitchStat() {
        return switchStat;
    }

    public void setSwitchStat(int switchStat) {
        this.switchStat = switchStat;
    }

    public Map<String, HostInfo> getHostMap() {
        return hostMap;
    }

    public void setHostMap(Map<String, HostInfo> hostMap) {
        this.hostMap = hostMap;
    }
    public boolean isNodesEmpty() {
        return this.hostMap.isEmpty();
    }

    public int getSize() {
        return hostMap.size();
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public boolean isInterVisit() {
        return isInterVisit;
    }

    public void setInterVisit(boolean interVisit) {
        isInterVisit = interVisit;
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("clusterId", clusterId)
                .append("groupId", groupId)
                .append("hostMap", hostMap)
                .append("load", load)
                .append("switchStat", switchStat)
                .append("isInterVisit", isInterVisit)
                .append("maxPacketLength", maxPacketLength)
                .toString();
    }
}
