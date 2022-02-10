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

package org.apache.inlong.sort.protocol.sink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.SerializationInfo;

import java.util.Objects;

public class KafkaSinkInfo extends SinkInfo {

    private static final long serialVersionUID = 161617117094475954L;

    @JsonProperty("address")
    private final String address;

    @JsonProperty("topic")
    private final String topic;

    @JsonProperty("serialization_info")
    private final SerializationInfo serializationInfo;

    @JsonCreator
    public KafkaSinkInfo(
            @JsonProperty("fields") FieldInfo[] fields,
            @JsonProperty("address") String address,
            @JsonProperty("topic") String topic,
            @JsonProperty("serialization_info") SerializationInfo serializationInfo
    ) {
        super(fields);
        this.address = address;
        this.topic = topic;
        this.serializationInfo = serializationInfo;
    }

    @JsonProperty("address")
    public String getAddress() {
        return address;
    }

    @JsonProperty("topic")
    public String getTopic() {
        return topic;
    }

    @JsonProperty("serialization_info")
    public SerializationInfo getSerializationInfo() {
        return serializationInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        KafkaSinkInfo that = (KafkaSinkInfo) o;
        return Objects.equals(address, that.address)
                && Objects.equals(topic, that.topic)
                && Objects.equals(serializationInfo, that.serializationInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), address, topic, serializationInfo);
    }
}
