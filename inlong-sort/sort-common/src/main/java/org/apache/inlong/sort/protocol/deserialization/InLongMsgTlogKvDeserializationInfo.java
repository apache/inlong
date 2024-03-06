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

/**
 * It represents TLog KV format of InLongMsg(m=15).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class InLongMsgTlogKvDeserializationInfo extends InLongMsgDeserializationInfo {

    private static final long serialVersionUID = 3299931901024581425L;

    private final char delimiter;

    private final char entryDelimiter;

    private final char kvDelimiter;

    @JsonInclude(Include.NON_NULL)
    @Nullable
    private final Character escapeChar;

    public InLongMsgTlogKvDeserializationInfo(
            @JsonProperty("streamId") @JsonAlias(value = {"tid"}) String streamId,
            @JsonProperty("delimiter") char delimiter,
            @JsonProperty("entry_delimiter") char entryDelimiter,
            @JsonProperty("kv_delimiter") char kvDelimiter) {
        this(streamId, delimiter, entryDelimiter, kvDelimiter, null);
    }

    @JsonCreator
    public InLongMsgTlogKvDeserializationInfo(
            @JsonProperty("streamId") @JsonAlias(value = {"tid"}) String streamId,
            @JsonProperty("delimiter") char delimiter,
            @JsonProperty("entry_delimiter") char entryDelimiter,
            @JsonProperty("kv_delimiter") char kvDelimiter,
            @JsonProperty("escape_char") @Nullable Character escapeChar) {
        super(streamId);
        this.delimiter = delimiter;
        this.entryDelimiter = entryDelimiter;
        this.kvDelimiter = kvDelimiter;
        this.escapeChar = escapeChar;
    }

    @JsonProperty("delimiter")
    public char getDelimiter() {
        return delimiter;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InLongMsgTlogKvDeserializationInfo other = (InLongMsgTlogKvDeserializationInfo) o;
        return super.equals(other)
                && delimiter == other.delimiter
                && entryDelimiter == other.entryDelimiter
                && kvDelimiter == other.kvDelimiter
                && Objects.equals(escapeChar, other.escapeChar);
    }
}