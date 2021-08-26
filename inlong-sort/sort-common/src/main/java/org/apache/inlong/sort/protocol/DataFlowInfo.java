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

package org.apache.inlong.sort.protocol;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.inlong.sort.protocol.sink.SinkInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;

/**
 * Data flow protocol.
 */
public class DataFlowInfo implements Serializable {

    private static final long serialVersionUID = 6549803844655330833L;

    private final long id;

    private final SourceInfo sourceInfo;

    private final SinkInfo sinkInfo;

    @JsonInclude(Include.NON_NULL)
    private final Map<String, Object> properties;

    @JsonCreator
    public DataFlowInfo(
            @JsonProperty("id") long id,
            @JsonProperty("source_info") SourceInfo sourceInfo,
            @JsonProperty("sink_info") SinkInfo sinkInfo,
            @JsonProperty("properties") Map<String, Object> properties) {
        this.id = id;
        this.sourceInfo = checkNotNull(sourceInfo);
        this.sinkInfo = checkNotNull(sinkInfo);
        this.properties = properties == null ? new HashMap<>() : properties;
    }

    public DataFlowInfo(long id, SourceInfo sourceInfo, SinkInfo sinkInfo) {
        this.id = id;
        this.sourceInfo = sourceInfo;
        this.sinkInfo = sinkInfo;
        this.properties = new HashMap<>();
    }

    @JsonProperty("id")
    public long getId() {
        return id;
    }

    @JsonProperty("source_info")
    public SourceInfo getSourceInfo() {
        return sourceInfo;
    }

    @JsonProperty("sink_info")
    public SinkInfo getSinkInfo() {
        return sinkInfo;
    }

    @JsonProperty("properties")
    public Map<String, Object> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataFlowInfo other = (DataFlowInfo) o;

        return Objects.equals(id, other.id)
                && Objects.equals(sourceInfo, other.sourceInfo)
                && Objects.equals(sinkInfo, other.sinkInfo)
                && Objects.equals(properties, other.properties);
    }
}
