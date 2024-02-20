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

import org.apache.inlong.sort.formats.base.DefaultSerializationSchema;
import org.apache.inlong.sort.formats.base.TextFormatOptions.MapNullKeyMode;
import org.apache.inlong.sort.formats.base.TextFormatOptions.TimestampFormat;
import org.apache.inlong.sort.formats.json.RowDataToFieldConverters.RowDataToFieldConverter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.Map;
import java.util.Objects;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.CHARSET;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.MAP_NULL_KEY_LITERAL;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.MAP_NULL_KEY_MODE;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.TIMESTAMP_FORMAT;
import static org.apache.inlong.sort.formats.base.TextFormatOptionsUtil.getMapNullKeyMode;
import static org.apache.inlong.sort.formats.base.TextFormatOptionsUtil.getTimestampFormat;

/**
 * Serialization schema that serializes an object of Flink internal data structure into a JSON
 * bytes.
 *
 * <p>Serializes the input Flink object into a JSON string and converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link
 * JsonRowDataDeserializationSchema}.
 */
@Internal
public class JsonRowDataSerializationSchema extends DefaultSerializationSchema<RowData> {

    private static final long serialVersionUID = 3483120986849438614L;

    /**
     * RowType to generate the runtime converter.
     */
    private final RowType rowType;

    /**
     * The converter that converts internal data formats to JsonNode.
     */
    private final RowDataToFieldConverter runtimeConverter;

    /**
     * Object mapper that is used to create output JSON objects.
     */
    private final ObjectMapper objectMapper;

    /**
     * Timestamp format specification which is used to parse timestamp.
     */
    private final TimestampFormat timestampFormat;

    /**
     * The handling mode when serializing null keys for map data.
     */
    private final MapNullKeyMode mapNullKeyMode;

    /**
     * The string literal when handling mode for map null key LITERAL.
     */
    private final String mapNullKeyLiteral;

    private final String charset;

    public JsonRowDataSerializationSchema(
            RowType rowType,
            TimestampFormat timestampFormat,
            MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            String charset,
            ObjectMapper objectMapper) {
        this(rowType, timestampFormat, mapNullKeyMode, mapNullKeyLiteral, charset, objectMapper, DEFAULT_IGNORE_ERRORS);
    }

    public JsonRowDataSerializationSchema(
            RowType rowType,
            TimestampFormat timestampFormat,
            MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            String charset,
            ObjectMapper objectMapper,
            boolean ignoreErrors) {
        super(ignoreErrors);
        this.rowType = rowType;
        this.timestampFormat = timestampFormat;
        this.mapNullKeyMode = mapNullKeyMode;
        this.mapNullKeyLiteral = mapNullKeyLiteral;
        this.charset = charset;
        this.objectMapper = objectMapper;
        this.runtimeConverter =
                new RowDataToFieldConverters(timestampFormat, mapNullKeyMode, mapNullKeyLiteral)
                        .createConverter(rowType);
    }

    public static Builder builder(RowType rowType) {
        return new Builder(rowType);
    }

    @Override
    public byte[] serializeInternal(RowData rowData) {
        try {
            // noinspection unchecked
            Map<String, Object> outputMap = (Map<String, Object>) runtimeConverter.convert(rowData);
            return objectMapper.writeValueAsString(outputMap).getBytes(charset);
        } catch (Throwable t) {
            throw new RuntimeException(String.format("Could not serialize rowData '%s'.", rowData), t);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JsonRowDataSerializationSchema that = (JsonRowDataSerializationSchema) o;
        return rowType.equals(that.rowType)
                && timestampFormat.equals(that.timestampFormat)
                && mapNullKeyMode.equals(that.mapNullKeyMode)
                && mapNullKeyLiteral.equals(that.mapNullKeyLiteral);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, timestampFormat, mapNullKeyMode, mapNullKeyLiteral);
    }

    /**
     * Builder for {@link JsonRowDataSerializationSchema}.
     */
    public static class Builder {

        private final RowType rowType;

        private TimestampFormat timestampFormat = getTimestampFormat(TIMESTAMP_FORMAT.defaultValue());

        private MapNullKeyMode mapNullKeyMode = getMapNullKeyMode(MAP_NULL_KEY_MODE.defaultValue());

        private String mapNullKeyLiteral = MAP_NULL_KEY_LITERAL.defaultValue();

        private String charset = CHARSET.defaultValue();

        private ObjectMapper objectMapper = new ObjectMapper();

        private boolean ignoreErrors = DEFAULT_IGNORE_ERRORS;

        private Builder(RowType rowType) {
            this.rowType = rowType;
        }

        public Builder setTimestampFormat(String timestampFormat) {
            this.timestampFormat = getTimestampFormat(timestampFormat);
            return this;
        }

        public Builder setMapNullKeyMode(String mapNullKeyMode) {
            this.mapNullKeyMode = getMapNullKeyMode(mapNullKeyMode);
            return this;
        }

        public Builder setMapNullKeyLiteral(String mapNullKeyLiteral) {
            this.mapNullKeyLiteral = mapNullKeyLiteral;
            return this;
        }

        public Builder setCharset(String charset) {
            this.charset = charset;
            return this;
        }

        public Builder setObjectMapperConfig(ReadableConfig config) {
            this.objectMapper = JsonFormatUtils.createObjectMapper(config);
            return this;
        }

        public Builder setIgnoreErrors(boolean ingoreErrors) {
            this.ignoreErrors = ingoreErrors;
            return this;
        }

        public JsonRowDataSerializationSchema build() {
            return new JsonRowDataSerializationSchema(
                    rowType,
                    timestampFormat,
                    mapNullKeyMode,
                    mapNullKeyLiteral,
                    charset,
                    objectMapper,
                    ignoreErrors);
        }
    }
}
