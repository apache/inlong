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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * It represents CSV2 format of InLongMsg(m=9).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class InLongMsgCsv2DeserializationInfo extends InLongMsgDeserializationInfo {

    private static final long serialVersionUID = 2188769102604850019L;

    private final char delimiter;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Character escapeChar;

    public InLongMsgCsv2DeserializationInfo(
            @JsonProperty("streamId") @JsonAlias(value = {"tid"}) String streamId,
            @JsonProperty("delimiter") char delimiter) {
        this(streamId, delimiter, null);
    }

    @JsonCreator
    public InLongMsgCsv2DeserializationInfo(
            @JsonProperty("streamId") @JsonAlias(value = {"tid"}) String streamId,
            @JsonProperty("delimiter") char delimiter,
            @JsonProperty("escape_char") @Nullable Character escapeChar) {
        super(streamId);
        this.delimiter = delimiter;
        this.escapeChar = escapeChar;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InLongMsgCsv2DeserializationInfo other = (InLongMsgCsv2DeserializationInfo) o;
        return super.equals(other)
                && delimiter == other.delimiter
                && Objects.equals(escapeChar, other.escapeChar);
    }

}