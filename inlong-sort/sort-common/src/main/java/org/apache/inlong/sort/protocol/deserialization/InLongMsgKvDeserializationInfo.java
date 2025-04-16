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
 * It represents KV format of InLongMsg(m=5).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class InLongMsgKvDeserializationInfo extends InLongMsgDeserializationInfo {

    private static final long serialVersionUID = 8431516458466278968L;

    private final char entryDelimiter;

    private final char kvDelimiter;

    @JsonInclude(Include.NON_NULL)
    @Nullable
    private final Character escapeChar;

    @JsonInclude(Include.NON_NULL)
    @Nullable
    private final Character lineDelimiter;

    @JsonProperty("delete_escape_char_while_deserialize")
    @Nullable
    private final Boolean deleteEscapeCharWhileDes;

    @JsonProperty("auto_append_escape_char_after_deserialize")
    @Nullable
    private final Boolean autoAppendEscapeCharAfterDes;

    public InLongMsgKvDeserializationInfo(
            @JsonProperty("streamId") @JsonAlias(value = {"tid"}) String streamId,
            @JsonProperty("entry_delimiter") char entryDelimiter,
            @JsonProperty("kv_delimiter") char kvDelimiter) {
        this(streamId, entryDelimiter, kvDelimiter, null, null);
    }

    @JsonCreator
    public InLongMsgKvDeserializationInfo(
            @JsonProperty("streamId") @JsonAlias(value = {"tid"}) String streamId,
            @JsonProperty("entry_delimiter") char entryDelimiter,
            @JsonProperty("kv_delimiter") char kvDelimiter,
            @JsonProperty("escape_char") @Nullable Character escapeChar,
            @JsonProperty("line_delimiter") @Nullable Character lineDelimiter) {
        this(streamId, entryDelimiter, kvDelimiter, escapeChar, lineDelimiter,
                DELETE_ESCAPE_CHAR_WHILE_DESERIALIZE_DEFAULT,
                AUTO_APPEND_ESCAPE_CHAR_AFTER_DESERIALIZE_DEFAULT);
    }

    @JsonCreator
    public InLongMsgKvDeserializationInfo(
            @JsonProperty("streamId") @JsonAlias(value = {"tid"}) String streamId,
            @JsonProperty("entry_delimiter") char entryDelimiter,
            @JsonProperty("kv_delimiter") char kvDelimiter,
            @JsonProperty("escape_char") @Nullable Character escapeChar,
            @JsonProperty("line_delimiter") @Nullable Character lineDelimiter,
            @JsonProperty("delete_escape_char_while_deserialize") @Nullable Boolean deleteEscapeCharWhileDes,
            @JsonProperty("auto_append_escape_char_after_deserialize") @Nullable Boolean autoAppendEscapeCharAfterDes) {
        super(streamId);
        this.entryDelimiter = entryDelimiter;
        this.kvDelimiter = kvDelimiter;
        this.escapeChar = escapeChar;
        this.lineDelimiter = lineDelimiter == null ? '\n' : lineDelimiter;
        this.deleteEscapeCharWhileDes = deleteEscapeCharWhileDes;
        this.autoAppendEscapeCharAfterDes = autoAppendEscapeCharAfterDes;
    }

    @JsonProperty("entry_delimiter")
    public char getEntryDelimiter() {
        return entryDelimiter;
    }

    @JsonProperty("kv_delimiter")
    public char getKvDelimiter() {
        return kvDelimiter;
    }

    @JsonProperty("escape_char")
    @Nullable
    public Character getEscapeChar() {
        return escapeChar;
    }

    @JsonProperty("line_delimiter")
    @Nullable
    public Character getLineDelimiter() {
        return lineDelimiter;
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

        InLongMsgKvDeserializationInfo other = (InLongMsgKvDeserializationInfo) o;
        return super.equals(other)
                && entryDelimiter == other.entryDelimiter
                && kvDelimiter == other.kvDelimiter
                && Objects.equals(escapeChar, other.escapeChar)
                && Objects.equals(lineDelimiter, other.lineDelimiter)
                && Objects.equals(deleteEscapeCharWhileDes, other.deleteEscapeCharWhileDes)
                && Objects.equals(autoAppendEscapeCharAfterDes, other.autoAppendEscapeCharAfterDes);
    }
}