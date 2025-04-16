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

import static org.apache.inlong.sort.protocol.constant.Constant.AUTO_APPEND_ESCAPE_CHAR_AFTER_DESERIALIZE_DEFAULT;
import static org.apache.inlong.sort.protocol.constant.Constant.DELETE_ESCAPE_CHAR_WHILE_DESERIALIZE_DEFAULT;

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

    @JsonProperty("delete_escape_char_while_deserialize")
    @Nullable
    private final Boolean deleteEscapeCharWhileDes;

    @JsonProperty("auto_append_escape_char_after_deserialize")
    @Nullable
    private final Boolean autoAppendEscapeCharAfterDes;

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
        this(streamId, entrySplitter, kvSplitter, escapeChar,
                DELETE_ESCAPE_CHAR_WHILE_DESERIALIZE_DEFAULT,
                AUTO_APPEND_ESCAPE_CHAR_AFTER_DESERIALIZE_DEFAULT);
    }

    @JsonCreator
    public KvDeserializationInfo(
            @JsonProperty("streamId") @JsonAlias(value = {"tid"}) String streamId,
            @JsonProperty("entry_splitter") char entrySplitter,
            @JsonProperty("kv_splitter") char kvSplitter,
            @JsonProperty("escape_char") @Nullable Character escapeChar,
            @JsonProperty("delete_escape_char_while_deserialize") @Nullable Boolean deleteEscapeCharWhileDes,
            @JsonProperty("auto_append_escape_char_after_deserialize") @Nullable Boolean autoAppendEscapeCharAfterDes) {
        super(streamId);
        this.streamId = (StringUtils.isEmpty(streamId) ? STREAM_ID_DEFAULT_VALUE : streamId);
        this.entrySplitter = entrySplitter;
        this.kvSplitter = kvSplitter;
        this.escapeChar = escapeChar;
        this.deleteEscapeCharWhileDes = deleteEscapeCharWhileDes;
        this.autoAppendEscapeCharAfterDes = autoAppendEscapeCharAfterDes;
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

    @JsonProperty("delete_escape_char_while_deserialize")
    @Nullable
    public Boolean getDeleteEscapeCharWhileDes() {
        if (deleteEscapeCharWhileDes != null) {
            return deleteEscapeCharWhileDes;
        }
        return DELETE_ESCAPE_CHAR_WHILE_DESERIALIZE_DEFAULT;
    }

    @JsonProperty("auto_append_escape_char_after_deserialize")
    @Nullable
    public Boolean getAutoAppendEscapeCharAfterDes() {
        if (autoAppendEscapeCharAfterDes != null) {
            return autoAppendEscapeCharAfterDes;
        }
        return AUTO_APPEND_ESCAPE_CHAR_AFTER_DESERIALIZE_DEFAULT;
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
                && Objects.equals(escapeChar, other.escapeChar)
                && Objects.equals(deleteEscapeCharWhileDes, other.deleteEscapeCharWhileDes)
                && Objects.equals(autoAppendEscapeCharAfterDes, other.autoAppendEscapeCharAfterDes);
    }
}
