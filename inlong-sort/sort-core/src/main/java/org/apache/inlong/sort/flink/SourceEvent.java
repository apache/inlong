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

package org.apache.inlong.sort.flink;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Map;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.inlong.sort.protocol.source.PulsarSourceInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;

public class SourceEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private final SourceEventType sourceEventType;

    private final long dataFlowId;

    private final SourceInfo sourceInfo;

    private final Map<String, Object> properties;

    public SourceEvent(SourceEventType sourceEventType, long dataFlowId, PulsarSourceInfo pulsarSourceInfo,
            Map<String, Object> properties) {
        this.sourceEventType = sourceEventType;
        this.dataFlowId = dataFlowId;
        this.sourceInfo = checkNotNull(pulsarSourceInfo);
        this.properties = properties;
    }

    public SourceEventType getSourceEventType() {
        return sourceEventType;
    }

    public SourceInfo getSourceInfo() {
        return sourceInfo;
    }

    public long getDataFlowId() {
        return dataFlowId;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("sourceEventType", sourceEventType)
                .append("dataFlowId", dataFlowId)
                .append("sourceInfo", sourceInfo)
                .append("properties", properties)
                .toString();
    }

    public enum SourceEventType {
        ADDED,
        UPDATE,
        REMOVED
    }
}


