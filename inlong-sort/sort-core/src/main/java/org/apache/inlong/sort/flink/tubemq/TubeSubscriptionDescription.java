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

package org.apache.inlong.sort.flink.tubemq;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import org.apache.inlong.sort.protocol.deserialization.InLongMsgDeserializationInfo;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * It represents a subscription information of tubeMQ topic.
 */
public class TubeSubscriptionDescription implements Serializable {

    private static final long serialVersionUID = 425839287278019795L;

    private final String topic;

    private String masterAddress;

    @Nullable
    private String consumerGroup;

    /**
     * Each topic might include multiple data flow if it's packed by InLongMsg.
     */
    private final Map<Long, TubeDataFlowDescription> dataFlows = new HashMap<>();

    public TubeSubscriptionDescription(String topic, String masterAddress, @Nullable String consumerGroup) {
        this.topic = checkNotNull(topic);
        this.masterAddress = checkNotNull(masterAddress);
        this.consumerGroup = consumerGroup;
    }

    public TubeSubscriptionDescription(TubeSubscriptionDescription other) {
        this.topic = other.topic;
        this.masterAddress = other.masterAddress;
        this.consumerGroup = other.consumerGroup;
        // TubeDataFlowDescription is immutable, so it's safe to keep the reference
        dataFlows.putAll(other.dataFlows);
    }

    public boolean isEmpty() {
        return dataFlows.isEmpty();
    }

    public void addDataFlow(long dataFlowId, TubeSourceInfo tubeSourceInfo) {
        // use the latter data flow to override master address and consumer group
        masterAddress = tubeSourceInfo.getMasterAddress();
        consumerGroup = tubeSourceInfo.getConsumerGroup();
        dataFlows.put(dataFlowId, TubeDataFlowDescription.generate(dataFlowId, tubeSourceInfo));
        // TODO, robustness check, there should not be conflict between data flows
    }

    public void removeDataFlow(long dataFlowId) {
        dataFlows.remove(dataFlowId);
    }

    public String getTopic() {
        return topic;
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    @Nullable
    public String getConsumerGroup() {
        return consumerGroup;
    }

    public Map<Long, TubeDataFlowDescription> getDataFlows() {
        return dataFlows;
    }

    public List<String> getTids() {
        // robustness check that empty description should not be used
        checkState(!dataFlows.isEmpty());
        return dataFlows
                .values()
                .stream()
                .filter(dataFlow -> dataFlow.tid != null)
                .map(dataFlow -> dataFlow.tid)
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        final TubeSubscriptionDescription other = (TubeSubscriptionDescription) obj;
        return Objects.equals(topic, other.topic)
                && Objects.equals(masterAddress, other.masterAddress)
                && Objects.equals(consumerGroup, other.consumerGroup)
                && Objects.equals(dataFlows, other.dataFlows);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("topic", topic)
                .append("masterAddress", masterAddress)
                .append("consumerGroup", consumerGroup)
                .append("dataFlows", dataFlows)
                .toString();
    }

    public static TubeSubscriptionDescription generate(long dataFlowId, TubeSourceInfo tubeSourceInfo) {
        final TubeSubscriptionDescription subscriptionInfo = new TubeSubscriptionDescription(tubeSourceInfo.getTopic(),
                tubeSourceInfo.getMasterAddress(), tubeSourceInfo.getConsumerGroup());
        subscriptionInfo.dataFlows
                .put(dataFlowId, TubeDataFlowDescription.generate(dataFlowId, tubeSourceInfo));
        return subscriptionInfo;
    }

    public static class TubeDataFlowDescription implements Serializable {

        private static final long serialVersionUID = 4822242395539034010L;

        private final long dataFlowId;

        /**
         * A data flow could be topic level or interface level.
         */
        @Nullable
        private final String tid;

        public TubeDataFlowDescription(long dataFlowId, @Nullable String tid) {
            this.dataFlowId = dataFlowId;
            this.tid = tid;
        }

        public long getDataFlowId() {
            return dataFlowId;
        }

        @Nullable
        public String getTid() {
            return tid;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            final TubeDataFlowDescription other = (TubeDataFlowDescription) obj;
            return Objects.equals(dataFlowId, other.dataFlowId) && Objects.equals(tid, other.tid);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .append("dataFlowId", dataFlowId)
                    .append("tid", tid)
                    .toString();
        }

        public static TubeDataFlowDescription generate(long dataFlowId, TubeSourceInfo tubeSourceInfo) {
            String tid = null;
            if (tubeSourceInfo.getDeserializationInfo() instanceof InLongMsgDeserializationInfo) {
                tid = ((InLongMsgDeserializationInfo) tubeSourceInfo.getDeserializationInfo()).getTid();
            }
            return new TubeDataFlowDescription(dataFlowId, tid);
        }
    }
}
