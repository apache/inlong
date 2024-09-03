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

package org.apache.inlong.sort.formats.kv;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.base.TableFormatForRowDataUtils;
import org.apache.inlong.sort.formats.base.TableFormatOptions;
import org.apache.inlong.sort.formats.base.TableFormatUtils;
import org.apache.inlong.sort.formats.base.TextFormatOptions;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static org.apache.inlong.sort.formats.base.TableFormatOptions.IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatOptions.ROW_FORMAT_INFO;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.deserializeRowFormatInfo;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.QUOTE_CHARACTER;

/**
 * Table format factory for providing configured instances of KV-to-RowData serializer and
 * deserializer.
 */
public class KvFormatFactory
        implements
            SerializationFormatFactory,
            DeserializationFormatFactory {

    public static final String IDENTIFIER = "inlong-kv";
    public static final String KV_PREFIX = IDENTIFIER + ".";

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context,
            ReadableConfig formatOptions) {

        FactoryUtil.validateFactoryOptions(this, formatOptions);
        KvCommons.validateFormatOptions(formatOptions);
        return new EncodingFormat<SerializationSchema<RowData>>() {

            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context,
                    DataType dataType) {

                final RowFormatInfo projectedRowFormatInfo = TableFormatForRowDataUtils.projectRowFormatInfo(
                        deserializeRowFormatInfo(formatOptions.get(ROW_FORMAT_INFO)),
                        dataType);
                KvRowDataSerializationSchema.Builder schemaBuilder =
                        new KvRowDataSerializationSchema.Builder(projectedRowFormatInfo);
                configureSerializationSchema(formatOptions, schemaBuilder);
                return schemaBuilder.build();
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context,
            ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        KvCommons.validateFormatOptions(formatOptions);

        return new DecodingFormat<DeserializationSchema<RowData>>() {

            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context,
                    DataType dataType) {
                KvRowDataDeserializationSchema.Builder schemaBuilder =
                        new KvRowDataDeserializationSchema.Builder(
                                TableFormatUtils.deriveRowFormatInfo(dataType),
                                context.createTypeInformation(dataType));
                configureDeserializationSchema(formatOptions, schemaBuilder);
                return schemaBuilder.build();
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
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TextFormatOptions.KV_ENTRY_DELIMITER);
        options.add(TextFormatOptions.KV_DELIMITER);
        options.add(TextFormatOptions.CHARSET);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ROW_FORMAT_INFO);
        options.add(TextFormatOptions.ESCAPE_CHARACTER);
        options.add(TextFormatOptions.QUOTE_CHARACTER);
        options.add(TextFormatOptions.NULL_LITERAL);
        options.add(TableFormatOptions.IGNORE_ERRORS);
        return options;
    }

    // ------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------

    private static void configureSerializationSchema(
            ReadableConfig formatOptions,
            KvRowDataSerializationSchema.Builder schemaBuilder) {

        formatOptions.getOptional(TextFormatOptions.CHARSET)
                .ifPresent(schemaBuilder::setCharset);

        formatOptions.getOptional(TextFormatOptions.KV_ENTRY_DELIMITER)
                .map(delimiter -> StringEscapeUtils.unescapeJava(delimiter).charAt(0))
                .ifPresent(schemaBuilder::setEntryDelimiter);

        formatOptions.getOptional(TextFormatOptions.KV_DELIMITER)
                .map(delimiter -> StringEscapeUtils.unescapeJava(delimiter).charAt(0))
                .ifPresent(schemaBuilder::setKvDelimiter);

        formatOptions
                .getOptional(ESCAPE_CHARACTER)
                .map(escape -> escape.charAt(0))
                .ifPresent(schemaBuilder::setEscapeCharacter);

        formatOptions
                .getOptional(QUOTE_CHARACTER)
                .map(quote -> quote.charAt(0))
                .ifPresent(schemaBuilder::setQuoteCharacter);

        formatOptions.getOptional(NULL_LITERAL)
                .ifPresent(schemaBuilder::setNullLiteral);

        formatOptions.getOptional(IGNORE_ERRORS)
                .ifPresent(schemaBuilder::setIgnoreErrors);

    }

    private static void configureDeserializationSchema(
            ReadableConfig formatOptions,
            KvRowDataDeserializationSchema.Builder schemaBuilder) {

        formatOptions.getOptional(TextFormatOptions.CHARSET)
                .ifPresent(schemaBuilder::setCharset);

        formatOptions.getOptional(TextFormatOptions.KV_ENTRY_DELIMITER)
                .map(delimiter -> StringEscapeUtils.unescapeJava(delimiter).charAt(0))
                .ifPresent(schemaBuilder::setEntryDelimiter);

        formatOptions.getOptional(TextFormatOptions.KV_DELIMITER)
                .map(delimiter -> StringEscapeUtils.unescapeJava(delimiter).charAt(0))
                .ifPresent(schemaBuilder::setKvDelimiter);

        formatOptions
                .getOptional(ESCAPE_CHARACTER)
                .map(escape -> escape.charAt(0))
                .ifPresent(schemaBuilder::setEscapeCharacter);

        formatOptions
                .getOptional(QUOTE_CHARACTER)
                .map(quote -> quote.charAt(0))
                .ifPresent(schemaBuilder::setQuoteCharacter);

        formatOptions.getOptional(NULL_LITERAL)
                .ifPresent(schemaBuilder::setNullLiteral);

        formatOptions.getOptional(IGNORE_ERRORS)
                .ifPresent(schemaBuilder::setIgnoreErrors);
    }
}
