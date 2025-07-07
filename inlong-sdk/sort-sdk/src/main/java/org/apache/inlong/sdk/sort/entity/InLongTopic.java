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

package org.apache.inlong.sdk.sort.entity;

import java.util.Map;
import java.util.Objects;

public class InLongTopic {

    private String topic;
    private CacheZoneCluster cacheZoneCluster;
    private int partitionId;
    // pulsar,kafka,tube
    private String topicType;
    private String startConsumeTime;
    private String stopConsumeTime;
    private Map<String, Object> properties;
    private String groupId;
    private String streamId;
    private String dataFlowId;

    public void setStopConsumeTime(String stopConsumeTime) {
        this.stopConsumeTime = stopConsumeTime;
    }

    public void setStartConsumeTime(String startConsumeTime) {
        this.startConsumeTime = startConsumeTime;
    }

    public String getStartConsumeTime() {
        return startConsumeTime;
    }

    public String getStopConsumeTime() {
        return stopConsumeTime;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public CacheZoneCluster getInLongCluster() {
        return cacheZoneCluster;
    }

    public void setInLongCluster(CacheZoneCluster cacheZoneCluster) {
        this.cacheZoneCluster = cacheZoneCluster;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public String getTopicType() {
        return topicType;
    }

    public void setTopicType(String topicType) {
        this.topicType = topicType;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    /**
     * get groupId
     * @return the groupId
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * set groupId
     * @param groupId the groupId to set
     */
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * get streamId
     * @return the streamId
     */
    public String getStreamId() {
        return streamId;
    }

    /**
     * set streamId
     * @param streamId the streamId to set
     */
    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    /**
     * get dataFlowId
     * @return the dataFlowId
     */
    public String getDataFlowId() {
        return dataFlowId;
    }

    /**
     * set dataFlowId
     * @param dataFlowId the dataFlowId to set
     */
    public void setDataFlowId(String dataFlowId) {
        this.dataFlowId = dataFlowId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InLongTopic that = (InLongTopic) o;
        return partitionId == that.partitionId
                && Objects.equals(topic, that.topic)
                && Objects.equals(cacheZoneCluster, that.cacheZoneCluster)
                && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, cacheZoneCluster, partitionId);
    }

    public String getTopicKey() {
        return cacheZoneCluster.getClusterId() + ":" + topic + ":" + partitionId;
    }

    @Override
    public String toString() {
        return "InLongTopic>>>" + topic + "|" + "|" + partitionId + "|" + topicType + "|" + cacheZoneCluster;
    }
}
