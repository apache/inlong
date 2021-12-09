/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.inlong.sort.entity;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class Topic implements Serializable {

    private String topic;
    private int partitionCnt;
    private Map<String, String> topicProperties;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartitionCnt() {
        return partitionCnt;
    }

    public void setPartitionCnt(int partitionCnt) {
        this.partitionCnt = partitionCnt;
    }

    public Map<String, String> getTopicProperties() {
        return topicProperties;
    }

    public void setTopicProperties(Map<String, String> topicProperties) {
        this.topicProperties = topicProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Topic topic1 = (Topic) o;
        return topic.equals(topic1.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic);
    }

    @Override
    public String toString() {
        return "Topic{"
                + "topic='" + topic + '\''
                + ", partitionCnt=" + partitionCnt
                + ", topicProperties=" + topicProperties
                + '}';
    }
}
