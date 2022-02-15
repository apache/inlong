package org.apache.inlong.sort.protocol.deserialization;

import java.util.Objects;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class DebeziumDeserializationInfo implements RowDataDeserializationInfo {

    private static final long serialVersionUID = 1L;

    @JsonProperty("ignore_parse_errors")
    private final boolean ignoreParseErrors;

    @JsonProperty("timestamp_format_standard")
    private final String timestampFormatStandard;

    @JsonCreator
    public DebeziumDeserializationInfo(
            @JsonProperty("ignore_parse_errors") boolean ignoreParseErrors,
            @JsonProperty("timestamp_format_standard") String timestampFormatStandard) {
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormatStandard = timestampFormatStandard;
    }

    @JsonProperty("ignore_parse_errors")
    public boolean isIgnoreParseErrors() {
        return ignoreParseErrors;
    }

    @JsonProperty("timestamp_format_standard")
    public String getTimestampFormatStandard() {
        return timestampFormatStandard;
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
        return ignoreParseErrors == that.ignoreParseErrors &&
                Objects.equals(timestampFormatStandard, that.timestampFormatStandard);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ignoreParseErrors, timestampFormatStandard);
    }

    @Override
    public String toString() {
        return "DebeziumDeserializationInfo{" +
                ", ignoreParseErrors=" + ignoreParseErrors +
                ", timestampFormatStandard='" + timestampFormatStandard + '\'' +
                '}';
    }
}
