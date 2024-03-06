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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAlias;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Kv deserialization info
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KvDeserializationInfo extends InLongMsgDeserializationInfo {

    private static final long serialVersionUID = -3182881360079888043L;

    private final char entrySplitter;

    private final char kvSplitter;

    private final String streamId;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Character escapeChar;

    public KvDeserializationInfo(
            @JsonProperty("entry_splitter") char entrySplitter,
            @JsonProperty("kv_splitter") char kvSplitter) {
        this(STREAM_ID_DEFAULT_VALUE, entrySplitter, kvSplitter, null);
    }

    public KvDeserializationInfo(
            @JsonProperty("entry_splitter") char entrySplitter,
            @JsonProperty("kv_splitter") char kvSplitter,
            @JsonProperty("escape_char") @Nullable Character escapeChar) {
        this(STREAM_ID_DEFAULT_VALUE, entrySplitter, kvSplitter, escapeChar);
    }

    @JsonCreator
    public KvDeserializationInfo(
            @JsonProperty("streamId") @JsonAlias(value = {"tid"}) String streamId,
            @JsonProperty("entry_splitter") char entrySplitter,
            @JsonProperty("kv_splitter") char kvSplitter,
            @JsonProperty("escape_char") @Nullable Character escapeChar) {
        super(streamId);
        this.streamId = (StringUtils.isEmpty(streamId) ? STREAM_ID_DEFAULT_VALUE : streamId);
        this.entrySplitter = entrySplitter;
        this.kvSplitter = kvSplitter;
        this.escapeChar = escapeChar;
    }

    @JsonProperty("entry_splitter")
    public char getEntrySplitter() {
        return entrySplitter;
    }

    @JsonProperty("kv_splitter")
    public char getKvSplitter() {
        return kvSplitter;
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

        KvDeserializationInfo other = (KvDeserializationInfo) o;
        return Objects.equals(streamId, other.getStreamId()) && entrySplitter == other.entrySplitter
                && kvSplitter == other.kvSplitter
                && Objects.equals(escapeChar, other.escapeChar);
    }
}
