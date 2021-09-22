/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.config;

import java.util.Map;

public class ProxyConfigEntry implements java.io.Serializable {
    private String clusterId;
    private String bid;
    private int size;
    private Map<String, HostInfo> hostMap;
    private int bidNum;
    private Map<String, Integer> tidNumMap;
    private int load;
    private int switchStat;
    private boolean isInterVisit;

    public int getLoad() {
        return load;
    }

    public void setLoad(int load) {
        this.load = load;
    }

    public int getBidNum() {
        return bidNum;
    }

    public Map<String, Integer> getTidNumMap() {
        return tidNumMap;
    }

    public void setBidNumAndTidNumMap(int bidNum, Map<String, Integer> tidNumMap) {
        this.bidNum = bidNum;
        this.tidNumMap = tidNumMap;
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
        this.size = hostMap.size();
        this.hostMap = hostMap;
    }

    public int getSize() {
        return size;
    }

    public String getBid() {
        return bid;
    }

    public void setBid(String bid) {
        this.bid = bid;
    }

    public boolean isInterVisit() {
        return isInterVisit;
    }

    public void setInterVisit(boolean interVisit) {
        isInterVisit = interVisit;
    }

    @Override
    public String toString() {
        return "ProxyConfigEntry [hostMap=" + hostMap + ", load=" + load + ", bsn=" + bidNum + ", tsnMap=" + tidNumMap
                + ", size=" + size + ", isInterVisit=" + isInterVisit + ", bid=" + bid
                + ", switch=" + switchStat + "]";
    }


    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }
}
