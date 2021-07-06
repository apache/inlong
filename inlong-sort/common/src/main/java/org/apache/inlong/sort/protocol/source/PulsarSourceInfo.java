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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;

public class PulsarSourceInfo extends SourceInfo {

    private static final long serialVersionUID = -3354319497859179290L;

    private final String adminUrl;

    private final String serviceUrl;

    private final String topic;

    private final String subscriptionName;

    @JsonCreator
    public PulsarSourceInfo(
            @JsonProperty("admin_url") String adminUrl,
            @JsonProperty("service_url") String serviceUrl,
            @JsonProperty("topic") String topic,
            @JsonProperty("subscription_name") String subscriptionName,
            @JsonProperty("deserialization_info") DeserializationInfo deserializationInfo,
            @JsonProperty("fields") FieldInfo[] fields) {
        super(fields, deserializationInfo);

        this.adminUrl = checkNotNull(adminUrl);
        this.serviceUrl = checkNotNull(serviceUrl);
        this.topic = checkNotNull(topic);
        this.subscriptionName = checkNotNull(subscriptionName);
    }

    @JsonProperty("admin_url")
    public String getAdminUrl() {
        return adminUrl;
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
}
