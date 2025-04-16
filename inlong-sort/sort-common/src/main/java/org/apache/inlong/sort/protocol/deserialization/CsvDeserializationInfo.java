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

import static org.apache.inlong.sort.protocol.constant.Constant.AUTO_APPEND_ESCAPE_CHAR_AFTER_DESERIALIZE_DEFAULT;
import static org.apache.inlong.sort.protocol.constant.Constant.DELETE_ESCAPE_CHAR_WHILE_DESERIALIZE_DEFAULT;

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

    @JsonProperty("delete_escape_char_while_deserialize")
    @Nullable
    private final Boolean deleteEscapeCharWhileDes;

    @JsonProperty("auto_append_escape_char_after_deserialize")
    @Nullable
    private final Boolean autoAppendEscapeCharAfterDes;

    // TODO: support mapping index to field
    public CsvDeserializationInfo(
            @JsonProperty("splitter") char splitter) {
        this(STREAM_ID_DEFAULT_VALUE, splitter, null,
                DELETE_ESCAPE_CHAR_WHILE_DESERIALIZE_DEFAULT,
                AUTO_APPEND_ESCAPE_CHAR_AFTER_DESERIALIZE_DEFAULT);
    }

    public CsvDeserializationInfo(
            @JsonProperty("splitter") char splitter,
            @JsonProperty("escape_char") @Nullable Character escapeChar) {
        this(STREAM_ID_DEFAULT_VALUE, splitter, escapeChar,
                DELETE_ESCAPE_CHAR_WHILE_DESERIALIZE_DEFAULT,
                AUTO_APPEND_ESCAPE_CHAR_AFTER_DESERIALIZE_DEFAULT);
    }

    @JsonCreator
    public CsvDeserializationInfo(
            @JsonProperty("streamId") String streamId,
            @JsonProperty("splitter") char splitter,
            @JsonProperty("escape_char") @Nullable Character escapeChar,
            @JsonProperty("delete_escape_char_while_deserialize") @Nullable Boolean deleteEscapeCharWhileDes,
            @JsonProperty("auto_append_escape_char_after_deserialize") @Nullable Boolean autoAppendEscapeCharAfterDes) {
        super(streamId);
        this.streamId = (StringUtils.isEmpty(streamId) ? STREAM_ID_DEFAULT_VALUE : streamId);
        this.splitter = splitter;
        this.escapeChar = escapeChar;
        this.deleteEscapeCharWhileDes = deleteEscapeCharWhileDes;
        this.autoAppendEscapeCharAfterDes = autoAppendEscapeCharAfterDes;
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

        CsvDeserializationInfo other = (CsvDeserializationInfo) o;
        return Objects.equals(streamId, other.getStreamId()) && splitter == other.splitter
                && Objects.equals(escapeChar, other.escapeChar)
                && Objects.equals(deleteEscapeCharWhileDes, other.deleteEscapeCharWhileDes)
                && Objects.equals(autoAppendEscapeCharAfterDes, other.autoAppendEscapeCharAfterDes);
    }

}
