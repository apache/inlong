/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.inlong.sort.base.metric;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.regex.Pattern;

import static org.apache.inlong.sort.base.Constants.DELIMITER;

public class MetricOption {
    private static final String IP_OR_HOST_PORT = "^(.*):([0-9]|[1-9]\\d|[1-9]\\d{"
            + "2}|[1-9]\\d{"
            + "3}|[1-5]\\d{"
            + "4}|6[0-4]\\d{"
            + "3}|65[0-4]\\d{"
            + "2}|655[0-2]\\d|6553[0-5])$";

    private String groupId;
    private String streamId;
    private String nodeId;
    private final HashSet<String> ipPortList;
    private String ipPorts;
    private RegisteredMetric registeredMetric;

    private MetricOption(
            String inLongGroupStreamNode,
            @Nullable String inLongAudit,
            @Nullable RegisteredMetric registeredMetric) {
        Preconditions.checkNotNull(inLongGroupStreamNode,
                "Inlong group stream node must be set for register metric.");
        if (inLongGroupStreamNode != null) {
            String[] inLongGroupStreamNodeArray = inLongGroupStreamNode.split(DELIMITER);
            Preconditions.checkArgument(inLongGroupStreamNodeArray.length == 3,
                    "Error inLong metric format: " + inLongGroupStreamNode);
            this.groupId = inLongGroupStreamNodeArray[0];
            this.streamId = inLongGroupStreamNodeArray[1];
            this.nodeId = inLongGroupStreamNodeArray[2];
        }

        this.ipPortList = new HashSet<>();
        this.ipPorts = null;
        if (inLongAudit != null) {
            String[] ipPortStrs = inLongAudit.split(DELIMITER);
            this.ipPorts = inLongAudit;
            for (String ipPort : ipPortStrs) {
                Preconditions.checkArgument(Pattern.matches(IP_OR_HOST_PORT, ipPort),
                        "Error inLong audit format: " + inLongAudit);
                this.ipPortList.add(ipPort);
            }
        }

        if (registeredMetric != null) {
            this.registeredMetric = registeredMetric;
        }
    }

    public String getGroupId() {
        return groupId;
    }

    public String getStreamId() {
        return streamId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public HashSet<String> getIpPortList() {
        return ipPortList;
    }

    public String getIpPorts() {
        return ipPorts;
    }

    public RegisteredMetric getRegisteredMetric() {
        return registeredMetric;
    }

    public static Builder builder() {
        return new Builder();
    }

    public enum RegisteredMetric {
        ALL,
        NORMAL,
        DIRTY
    }

    public static class Builder {
        private String inLongGroupStreamNode;
        private String inLongAudit;
        private RegisteredMetric registeredMetric = RegisteredMetric.ALL;

        private Builder() {
        }

        public MetricOption.Builder withInLongMetric(String inLongMetric) {
            this.inLongGroupStreamNode = inLongMetric;
            return this;
        }

        public MetricOption.Builder withInLongAudit(String inLongAudit) {
            this.inLongAudit = inLongAudit;
            return this;
        }

        public MetricOption.Builder withRegisterMetric(RegisteredMetric registeredMetric) {
            this.registeredMetric = registeredMetric;
            return this;
        }

        public MetricOption build() {
            if (inLongGroupStreamNode == null && inLongAudit == null) {
                return null;
            }
            return new MetricOption(inLongGroupStreamNode, inLongAudit, registeredMetric);
        }
    }
}
