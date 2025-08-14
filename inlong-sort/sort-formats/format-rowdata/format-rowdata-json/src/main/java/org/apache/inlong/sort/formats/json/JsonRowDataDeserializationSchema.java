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

package org.apache.inlong.sort.formats.json;

import org.apache.inlong.sort.formats.base.DefaultDeserializationSchema;
import org.apache.inlong.sort.formats.base.FormatMsg;
import org.apache.inlong.sort.formats.base.TextFormatOptions.TimestampFormat;
import org.apache.inlong.sort.formats.json.FieldToRowDataConverters.FieldToRowDataConverter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.inlong.sort.formats.base.TableFormatOptions.IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.CHARSET;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.FAIL_ON_MISSING_FIELD;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.TIMESTAMP_FORMAT;
import static org.apache.inlong.sort.formats.base.TextFormatOptionsUtil.getTimestampFormat;
import static org.apache.inlong.sort.formats.json.JsonFormatUtils.createObjectMapper;

/**
 * Deserialization schema from JSON to Flink Table/SQL internal data structure {@link RowData}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
public class JsonRowDataDeserializationSchema extends DefaultDeserializationSchema<RowData> {

    private static final long serialVersionUID = -236046395848412854L;

    /**
     * Flag indicating whether to fail if a field is missing.
     */
    private final boolean failOnMissingField;

    /**
     * TypeInformation of the produced {@link RowData}.
     */
    private final TypeInformation<RowData> resultTypeInfo;

    /**
     * Runtime converter that converts {@link JsonNode}s into objects of Flink SQL internal data
     * structures.
     */
    private final FieldToRowDataConverter runtimeConverter;

    /**
     * Timestamp format specification which is used to parse timestamp.
     */
    private final TimestampFormat timestampFormat;

    private final String charset;

    private JsonRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat,
            String charset,
            ObjectMapper objectMapper) {
        super(ignoreParseErrors);
        if (ignoreParseErrors && failOnMissingField) {
            throw new IllegalArgumentException(
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.failOnMissingField = failOnMissingField;
        this.charset = charset;
        this.runtimeConverter = new FieldToRowDataConverters(
                failOnMissingField,
                ignoreParseErrors,
                timestampFormat,
                objectMapper).createConverter(checkNotNull(rowType));
        this.timestampFormat = timestampFormat;
    }

    public static Builder builder(RowType rowType, TypeInformation<RowData> resultTypeInfo) {
        return new Builder(rowType, resultTypeInfo);
    }

    @Override
    public RowData deserializeInternal(@Nullable byte[] message) throws Exception {
        if (message == null) {
            return null;
        }

        String jsonStr = new String(message, charset);
        RowData rowData = null;
        try {
            rowData = (RowData) runtimeConverter.convert(jsonStr);
        } catch (Throwable t) {
            failureHandler.onParsingMsgFailure(jsonStr, new RuntimeException(
                    String.format("Could not properly deserialize json. Text=[%s].", jsonStr), t));
        }
        return rowData;
    }

    @Override
    public FormatMsg deserializeFormatMsg(byte[] message) throws Exception {
        if (message == null) {
            return null;
        }

        String jsonStr = new String(message, charset);
        RowData rowData = null;
        try {
            rowData = (RowData) runtimeConverter.convert(jsonStr);
        } catch (Throwable t) {
            failureHandler.onParsingMsgFailure(jsonStr, new RuntimeException(
                    String.format("Could not properly deserialize json. Text=[%s].", jsonStr), t));
        }
        return new FormatMsg(rowData, message.length);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        JsonRowDataDeserializationSchema that = (JsonRowDataDeserializationSchema) o;
        return failOnMissingField == that.failOnMissingField
                && resultTypeInfo.equals(that.resultTypeInfo)
                && timestampFormat.equals(that.timestampFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), failOnMissingField, resultTypeInfo, timestampFormat);
    }

    /**
     * Builder for {@link JsonRowDataDeserializationSchema}.
     */
    public static class Builder {

        private final RowType rowType;

        private final TypeInformation<RowData> resultTypeInfo;

        private ObjectMapper objectMapper = new ObjectMapper();

        private boolean failOnMissingField = FAIL_ON_MISSING_FIELD.defaultValue();

        private boolean ignoreParseErrors = IGNORE_ERRORS.defaultValue();

        private TimestampFormat timestampFormat = getTimestampFormat(TIMESTAMP_FORMAT.defaultValue());

        private String charset = CHARSET.defaultValue();

        private Builder(RowType rowType, TypeInformation<RowData> resultTypeInfo) {
            this.rowType = rowType;
            this.resultTypeInfo = resultTypeInfo;
        }

        public Builder setFailOnMissingField(boolean failOnMissingField) {
            this.failOnMissingField = failOnMissingField;
            return this;
        }

        public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
            this.ignoreParseErrors = ignoreParseErrors;
            return this;
        }

        public Builder setTimestampFormat(String timestampFormat) {
            this.timestampFormat = getTimestampFormat(timestampFormat);
            return this;
        }

        public Builder setCharset(String charset) {
            this.charset = charset;
            return this;
        }

        public Builder setObjectMapperConfig(ReadableConfig config) {
            this.objectMapper = createObjectMapper(config);
            return this;
        }

        public JsonRowDataDeserializationSchema build() {
            return new JsonRowDataDeserializationSchema(
                    rowType,
                    resultTypeInfo,
                    failOnMissingField,
                    ignoreParseErrors,
                    timestampFormat,
                    charset,
                    objectMapper);
        }
    }
}
