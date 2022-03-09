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

package org.apache.inlong.sort.protocol.source;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Objects;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;

public class TDMQPulsarSourceInfo extends SourceInfo {

    private static final long serialVersionUID = 1L;

    private final String serviceUrl;

    private final String topic;

    private final String subscriptionName;

    private final String authentication;

    @JsonCreator
    public TDMQPulsarSourceInfo(
            @JsonProperty("service_url") String serviceUrl,
            @JsonProperty("topic") String topic,
            @JsonProperty("subscription_name") String subscriptionName,
            @JsonProperty("authentication") String authentication,
            @JsonProperty("deserialization_info") DeserializationInfo deserializationInfo,
            @JsonProperty("fields") FieldInfo[] fields) {
        super(fields, deserializationInfo);

        this.serviceUrl = checkNotNull(serviceUrl);
        this.topic = checkNotNull(topic);
        this.subscriptionName = checkNotNull(subscriptionName);
        this.authentication = authentication;
    }

    @JsonProperty("service_url")
    public String getServiceUrl() {
        return serviceUrl;
    }

    @JsonProperty("topic")
    public String getTopic() {
        return topic;
    }

    @JsonProperty("subscription_name")
    public String getSubscriptionName() {
        return subscriptionName;
    }

    @JsonProperty("authentication")
    public String getAuthentication() {
        return authentication;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TDMQPulsarSourceInfo other = (TDMQPulsarSourceInfo) o;
        return super.equals(other)
                       && Objects.equals(serviceUrl, other.serviceUrl)
                       && Objects.equals(subscriptionName, other.subscriptionName)
                       && Objects.equals(topic, other.topic)
                       && Objects.equals(authentication, other.authentication);
    }
}
