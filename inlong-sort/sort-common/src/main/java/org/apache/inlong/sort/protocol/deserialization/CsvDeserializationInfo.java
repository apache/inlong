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

package org.apache.inlong.sort.protocol.deserialization;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Csv deserialization info
 */
public class CsvDeserializationInfo extends InLongMsgDeserializationInfo {

    private static final long serialVersionUID = 7424482369272150638L;

    private final char splitter;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Character escapeChar;

    private final String streamId;

    // TODO: support mapping index to field
    public CsvDeserializationInfo(
            @JsonProperty("splitter") char splitter) {
        this(STREAM_ID_DEFAULT_VALUE, splitter, null);
    }

    public CsvDeserializationInfo(
            @JsonProperty("splitter") char splitter,
            @JsonProperty("escape_char") @Nullable Character escapeChar) {
        this(STREAM_ID_DEFAULT_VALUE, splitter, escapeChar);
    }

    @JsonCreator
    public CsvDeserializationInfo(
            @JsonProperty("streamId") String streamId,
            @JsonProperty("splitter") char splitter,
            @JsonProperty("escape_char") @Nullable Character escapeChar) {
        super(streamId);
        this.streamId = (StringUtils.isEmpty(streamId) ? STREAM_ID_DEFAULT_VALUE : streamId);
        this.splitter = splitter;
        this.escapeChar = escapeChar;
    }

    @JsonProperty("splitter")
    public char getSplitter() {
        return splitter;
    }

    @JsonProperty("escape_char")
    @Nullable
    public Character getEscapeChar() {
        return escapeChar;
    }

    @JsonProperty("streamId")
    public String getStreamId() {
        return streamId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CsvDeserializationInfo other = (CsvDeserializationInfo) o;
        return Objects.equals(streamId, other.getStreamId()) && splitter == other.splitter
                && Objects.equals(escapeChar, other.escapeChar);
    }

}
