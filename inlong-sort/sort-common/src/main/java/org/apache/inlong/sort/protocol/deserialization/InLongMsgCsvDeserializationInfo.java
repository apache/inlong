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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAlias;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.inlong.sort.protocol.constant.Constant.AUTO_APPEND_ESCAPE_CHAR_AFTER_DESERIALIZE_DEFAULT;
import static org.apache.inlong.sort.protocol.constant.Constant.DELETE_ESCAPE_CHAR_WHILE_DESERIALIZE_DEFAULT;

/**
 * It represents CSV format of InLongMsg(m=0).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class InLongMsgCsvDeserializationInfo extends InLongMsgDeserializationInfo {

    private static final long serialVersionUID = 1499370571949888870L;

    private final char delimiter;

    @JsonInclude(Include.NON_NULL)
    @Nullable
    private final Character escapeChar;

    @JsonInclude(Include.NON_NULL)
    private final boolean deleteHeadDelimiter;

    @JsonProperty("delete_escape_char_while_deserialize")
    @Nullable
    private final Boolean deleteEscapeCharWhileDes;

    @JsonProperty("auto_append_escape_char_after_deserialize")
    @Nullable
    private final Boolean autoAppendEscapeCharAfterDes;

    public InLongMsgCsvDeserializationInfo(
            @JsonProperty("streamId") @JsonAlias(value = {"tid"}) String streamId,
            @JsonProperty("delimiter") char delimiter) {
        this(streamId, delimiter, null, false);
    }

    public InLongMsgCsvDeserializationInfo(
            @JsonProperty("streamId") @JsonAlias(value = {"tid"}) String streamId,
            @JsonProperty("delimiter") char delimiter,
            @JsonProperty("delete_head_delimiter") boolean deleteHeadDelimiter) {
        this(streamId, delimiter, null, deleteHeadDelimiter);
    }

    @JsonCreator
    public InLongMsgCsvDeserializationInfo(
            @JsonProperty("streamId") @JsonAlias(value = {"tid"}) String streamId,
            @JsonProperty("delimiter") char delimiter,
            @JsonProperty("escape_char") @Nullable Character escapeChar,
            @JsonProperty("delete_head_delimiter") boolean deleteHeadDelimiter) {
        this(streamId, delimiter, escapeChar, deleteHeadDelimiter,
                DELETE_ESCAPE_CHAR_WHILE_DESERIALIZE_DEFAULT,
                AUTO_APPEND_ESCAPE_CHAR_AFTER_DESERIALIZE_DEFAULT);
    }

    @JsonCreator
    public InLongMsgCsvDeserializationInfo(
            @JsonProperty("streamId") @JsonAlias(value = {"tid"}) String streamId,
            @JsonProperty("delimiter") char delimiter,
            @JsonProperty("escape_char") @Nullable Character escapeChar,
            @JsonProperty("delete_head_delimiter") boolean deleteHeadDelimiter,
            @JsonProperty("delete_escape_char_while_deserialize") @Nullable Boolean deleteEscapeCharWhileDes,
            @JsonProperty("auto_append_escape_char_after_deserialize") @Nullable Boolean autoAppendEscapeCharAfterDes) {
        super(streamId);
        this.delimiter = delimiter;
        this.escapeChar = escapeChar;
        this.deleteHeadDelimiter = deleteHeadDelimiter;
        this.deleteEscapeCharWhileDes = deleteEscapeCharWhileDes;
        this.autoAppendEscapeCharAfterDes = autoAppendEscapeCharAfterDes;
    }

    @JsonProperty("delimiter")
    public char getDelimiter() {
        return delimiter;
    }

    @JsonProperty("escape_char")
    @Nullable
    public Character getEscapeChar() {
        return escapeChar;
    }

    @JsonProperty("delete_head_delimiter")
    public boolean isDeleteHeadDelimiter() {
        return deleteHeadDelimiter;
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

        InLongMsgCsvDeserializationInfo other = (InLongMsgCsvDeserializationInfo) o;
        return super.equals(other)
                && delimiter == other.delimiter
                && Objects.equals(escapeChar, other.escapeChar)
                && deleteHeadDelimiter == other.deleteHeadDelimiter
                && Objects.equals(deleteEscapeCharWhileDes, other.deleteEscapeCharWhileDes)
                && Objects.equals(autoAppendEscapeCharAfterDes, other.autoAppendEscapeCharAfterDes);
    }
}
