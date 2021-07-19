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

import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * TubeMQ source information.
 */
public class TubeSourceInfo extends SourceInfo {

    private static final long serialVersionUID = 3559859089154486836L;

    private final String topic;

    private final String masterAddress;

    @JsonInclude(Include.NON_NULL)
    private final String consumerGroup;

    @JsonCreator
    public TubeSourceInfo(
            @JsonProperty("topic") String topic,
            @JsonProperty("master_address") String masterAddress,
            @JsonProperty("consumer_group") @Nullable String consumerGroup,
            @JsonProperty("deserialization_info") DeserializationInfo deserializationInfo,
            @JsonProperty("fields") FieldInfo[] fields) {
        super(fields, deserializationInfo);

        this.topic = checkNotNull(topic);
        this.masterAddress = checkNotNull(masterAddress);
        this.consumerGroup = consumerGroup;
    }

    @JsonProperty("topic")
    public String getTopic() {
        return topic;
    }

    @JsonProperty("master_address")
    public String getMasterAddress() {
        return masterAddress;
    }


    @JsonProperty("consumer_group")
    @Nullable
    public String getConsumerGroup() {
        return consumerGroup;
    }
}
