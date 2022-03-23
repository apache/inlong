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

import java.io.Serializable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;

/**
 * The base class of the data source in the metadata.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @Type(value = TubeSourceInfo.class, name = Constants.SOURCE_TYPE_TUBE),
        @Type(value = PulsarSourceInfo.class, name = Constants.SOURCE_TYPE_PULSAR),
        @Type(value = TDMQPulsarSourceInfo.class, name = Constants.SOURCE_TYPE_TDMQ_PULSAR)
})
public abstract class SourceInfo implements Serializable {

    private static final long serialVersionUID = 701717917118317501L;

    private final FieldInfo[] fields;

    private final DeserializationInfo deserializationInfo;

    public SourceInfo(
            @JsonProperty("fields") FieldInfo[] fields,
            @JsonProperty("deserialization_info") DeserializationInfo deserializationInfo) {
        this.fields = checkNotNull(fields);
        this.deserializationInfo = checkNotNull(deserializationInfo);
    }

    @JsonProperty("fields")
    public FieldInfo[] getFields() {
        return fields;
    }

    @JsonProperty("deserialization_info")
    public DeserializationInfo getDeserializationInfo() {
        return deserializationInfo;
    }
}
