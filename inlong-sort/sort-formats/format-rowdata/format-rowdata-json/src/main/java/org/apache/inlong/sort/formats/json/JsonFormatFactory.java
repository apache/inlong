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

import org.apache.inlong.sort.formats.base.TableFormatOptions;
import org.apache.inlong.sort.formats.base.TextFormatOptions;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.validateFactoryOptions;
import static org.apache.inlong.sort.formats.base.TextFormatOptionsUtil.validateDecodingFormatOptions;
import static org.apache.inlong.sort.formats.base.TextFormatOptionsUtil.validateEncodingFormatOptions;

/**
 * Table format factory for providing configured instances of JSON to RowData {@link
 * SerializationSchema} and {@link DeserializationSchema}.
 */
public class JsonFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "inlong-json";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            Context context,
            ReadableConfig formatOptions) {
        validateFactoryOptions(this, formatOptions);
        validateDecodingFormatOptions(formatOptions);

        return new DecodingFormat<DeserializationSchema<RowData>>() {

            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context,
                    DataType physicalDataType) {
                final RowType rowType = (RowType) physicalDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation(physicalDataType);
                JsonRowDataDeserializationSchema.Builder builder =
                        JsonRowDataDeserializationSchema.builder(rowType, rowDataTypeInfo);
                return builder
                        .setFailOnMissingField(formatOptions.get(TextFormatOptions.FAIL_ON_MISSING_FIELD))
                        .setIgnoreParseErrors(formatOptions.get(TableFormatOptions.IGNORE_ERRORS))
                        .setTimestampFormat(formatOptions.get(TextFormatOptions.TIMESTAMP_FORMAT))
                        .setCharset(formatOptions.get(TextFormatOptions.CHARSET))
                        .setObjectMapperConfig(formatOptions)
                        .build();
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            Context context,
            ReadableConfig formatOptions) {
        validateFactoryOptions(this, formatOptions);
        validateEncodingFormatOptions(formatOptions);

        return new EncodingFormat<SerializationSchema<RowData>>() {

            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context,
                    DataType physicalDataType) {
                RowType rowType = (RowType) physicalDataType.getLogicalType();
                JsonRowDataSerializationSchema.Builder builder = JsonRowDataSerializationSchema.builder(rowType);
                return builder
                        .setMapNullKeyMode(formatOptions.get(TextFormatOptions.MAP_NULL_KEY_MODE))
                        .setMapNullKeyLiteral(formatOptions.get(TextFormatOptions.MAP_NULL_KEY_LITERAL))
                        .setTimestampFormat(formatOptions.get(TextFormatOptions.TIMESTAMP_FORMAT))
                        .setCharset(formatOptions.get(TextFormatOptions.CHARSET))
                        .setIgnoreErrors(formatOptions.get(TableFormatOptions.IGNORE_ERRORS))
                        .setObjectMapperConfig(formatOptions)
                        .build();
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();

        // for text options
        options.add(TextFormatOptions.CHARSET);
        options.add(TextFormatOptions.FAIL_ON_MISSING_FIELD);
        options.add(TableFormatOptions.IGNORE_ERRORS);
        options.add(TextFormatOptions.TIMESTAMP_FORMAT);
        options.add(TextFormatOptions.MAP_NULL_KEY_MODE);
        options.add(TextFormatOptions.MAP_NULL_KEY_LITERAL);

        // for json ObjectMapper
        options.add(JsonFormatOptions.AUTO_CLOSE_SOURCE);
        options.add(JsonFormatOptions.ALLOW_COMMENTS);
        options.add(JsonFormatOptions.ALLOW_YAML_COMMENTS);
        options.add(JsonFormatOptions.ALLOW_UNQUOTED_FIELD_NAMES);
        options.add(JsonFormatOptions.ALLOW_SINGLE_QUOTES);
        options.add(JsonFormatOptions.ALLOW_UNQUOTED_CONTROL_CHARS);
        options.add(JsonFormatOptions.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER);
        options.add(JsonFormatOptions.ALLOW_NUMERIC_LEADING_ZEROS);
        options.add(JsonFormatOptions.ALLOW_NON_NUMERIC_NUMBERS);
        options.add(JsonFormatOptions.ALLOW_MISSING_VALUES);
        options.add(JsonFormatOptions.ALLOW_TRAILING_COMMA);
        options.add(JsonFormatOptions.STRICT_DUPLICATE_DETECTION);
        options.add(JsonFormatOptions.IGNORE_UNDEFINED);
        options.add(JsonFormatOptions.INCLUDE_SOURCE_IN_LOCATION);

        return options;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();

        options.add(TextFormatOptions.CHARSET);
        options.add(TextFormatOptions.TIMESTAMP_FORMAT);
        options.add(TextFormatOptions.MAP_NULL_KEY_MODE);
        options.add(TextFormatOptions.MAP_NULL_KEY_LITERAL);

        return options;
    }
}
