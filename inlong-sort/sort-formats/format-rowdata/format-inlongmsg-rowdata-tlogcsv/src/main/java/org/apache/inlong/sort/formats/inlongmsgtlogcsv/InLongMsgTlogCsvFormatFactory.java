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

package org.apache.inlong.sort.formats.inlongmsgtlogcsv;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.inlong.sort.formats.base.TableFormatOptions.ROW_FORMAT_INFO;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.deserializeRowFormatInfo;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.ATTRIBUTE_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.CHARSET;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.FIELD_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.NULL_LITERAL;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.QUOTE_CHARACTER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.TIME_FIELD_NAME;

/**
 * Table format factory for providing configured instances of InLongMsgTlogCsv-to-row
 * serializer and deserializer.
 */
public final class InLongMsgTlogCsvFormatFactory implements DeserializationFormatFactory {

    public static final String IDENTIFIER = "inlong-msg-tlogcsv";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateFormatOptions(formatOptions);

        return new DecodingFormat<DeserializationSchema<RowData>>() {

            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType producedDataType) {

                InLongMsgTlogCsvDeserializationSchema.Builder builder =
                        new InLongMsgTlogCsvDeserializationSchema.Builder(
                                deserializeRowFormatInfo(formatOptions.get(ROW_FORMAT_INFO)));
                configureDeserializationSchema(formatOptions, builder);
                return builder.build();
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
        return Stream.of(ROW_FORMAT_INFO).collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TIME_FIELD_NAME);
        options.add(ATTRIBUTE_FIELD_NAME);
        options.add(CHARSET);
        options.add(FIELD_DELIMITER);
        options.add(QUOTE_CHARACTER);
        options.add(ESCAPE_CHARACTER);
        options.add(NULL_LITERAL);
        options.add(IGNORE_ERRORS);
        return options;
    }

    // ------------------------------------------------------------------------
    // Validation
    // ------------------------------------------------------------------------

    static void validateFormatOptions(ReadableConfig tableOptions) {
        // Validate the option value must be a single char.
        validateCharacterVal(tableOptions, CHARSET);
        validateCharacterVal(tableOptions, TIME_FIELD_NAME);
        validateCharacterVal(tableOptions, ATTRIBUTE_FIELD_NAME);
        validateCharacterVal(tableOptions, FIELD_DELIMITER, true);
        validateCharacterVal(tableOptions, QUOTE_CHARACTER);
        validateCharacterVal(tableOptions, ESCAPE_CHARACTER);
    }

    /**
     * Validates the option {@code option} value must be a Character.
     */
    private static void validateCharacterVal(
            ReadableConfig tableOptions, ConfigOption<String> option) {
        validateCharacterVal(tableOptions, option, false);
    }

    /**
     * Validates the option {@code option} value must be a Character.
     *
     * @param tableOptions the table options
     * @param option       the config option
     * @param unescape     whether to unescape the option value
     */
    private static void validateCharacterVal(
            ReadableConfig tableOptions, ConfigOption<String> option, boolean unescape) {
        if (tableOptions.getOptional(option).isPresent()) {
            final String value =
                    unescape
                            ? StringEscapeUtils.unescapeJava(tableOptions.get(option))
                            : tableOptions.get(option);
            if (value.length() != 1) {
                throw new ValidationException(
                        String.format(
                                "Option '%s.%s' must be a string with single character, but was: %s",
                                IDENTIFIER, option.key(), tableOptions.get(option)));
            }
        }
    }
    // ------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------

    private static void configureDeserializationSchema(
            ReadableConfig formatOptions, InLongMsgTlogCsvDeserializationSchema.Builder schemaBuilder) {
        formatOptions
                .getOptional(TIME_FIELD_NAME)
                .ifPresent(schemaBuilder::setTimeFieldName);

        formatOptions
                .getOptional(ATTRIBUTE_FIELD_NAME)
                .ifPresent(schemaBuilder::setAttributesFieldName);

        formatOptions
                .getOptional(CHARSET)
                .ifPresent(schemaBuilder::setCharset);

        formatOptions
                .getOptional(FIELD_DELIMITER)
                .map(delimiter -> StringEscapeUtils.unescapeJava(delimiter).charAt(0))
                .ifPresent(schemaBuilder::setDelimiter);

        formatOptions
                .getOptional(QUOTE_CHARACTER)
                .map(quote -> quote.charAt(0))
                .ifPresent(schemaBuilder::setQuoteCharacter);

        formatOptions
                .getOptional(ESCAPE_CHARACTER)
                .map(escape -> escape.charAt(0))
                .ifPresent(schemaBuilder::setEscapeCharacter);

        formatOptions.getOptional(NULL_LITERAL).ifPresent(schemaBuilder::setNullLiteral);

        formatOptions.getOptional(IGNORE_ERRORS).ifPresent(schemaBuilder::setIgnoreErrors);
    }
}
