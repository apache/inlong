/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.protocol.deserialization;

import java.util.Objects;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class DebeziumDeserializationInfo implements DeserializationInfo {

    private static final long serialVersionUID = 1L;

    @JsonProperty("ignore_parse_errors")
    private final boolean ignoreParseErrors;

    @JsonProperty("timestamp_format_standard")
    private final String timestampFormatStandard;

    @JsonInclude(Include.NON_NULL)
    @JsonProperty("update_before_include")
    private final boolean updateBeforeInclude;

    @JsonCreator
    public DebeziumDeserializationInfo(
            @JsonProperty("ignore_parse_errors") boolean ignoreParseErrors,
            @JsonProperty("timestamp_format_standard") String timestampFormatStandard,
            @JsonProperty("update_before_include") boolean updateBeforeInclude) {
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormatStandard = timestampFormatStandard;
        this.updateBeforeInclude = updateBeforeInclude;
    }

    public DebeziumDeserializationInfo(boolean ignoreParseErrors, String timestampFormatStandard) {
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormatStandard = timestampFormatStandard;
        this.updateBeforeInclude = false;
    }

    @JsonProperty("ignore_parse_errors")
    public boolean isIgnoreParseErrors() {
        return ignoreParseErrors;
    }

    @JsonProperty("timestamp_format_standard")
    public String getTimestampFormatStandard() {
        return timestampFormatStandard;
    }

    @JsonProperty("update_before_include")
    public boolean isUpdateBeforeInclude() {
        return updateBeforeInclude;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DebeziumDeserializationInfo that = (DebeziumDeserializationInfo) o;
        return ignoreParseErrors == that.ignoreParseErrors && updateBeforeInclude == that.updateBeforeInclude
                && Objects.equals(timestampFormatStandard, that.timestampFormatStandard);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ignoreParseErrors, timestampFormatStandard, updateBeforeInclude);
    }

    @Override
    public String toString() {
        return "DebeziumDeserializationInfo{"
                + "ignoreParseErrors=" + ignoreParseErrors
                + ", timestampFormatStandard='" + timestampFormatStandard + '\''
                + ", updateBeforeInclude=" + updateBeforeInclude
                + '}';
    }
}
