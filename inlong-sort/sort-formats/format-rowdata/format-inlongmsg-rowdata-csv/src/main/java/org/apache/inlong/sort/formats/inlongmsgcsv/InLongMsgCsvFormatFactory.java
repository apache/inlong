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

package org.apache.inlong.sort.formats.inlongmsgcsv;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.inlong.sort.formats.base.TableFormatOptions.ROW_FORMAT_INFO;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.ATTRIBUTE_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.CHARSET;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.DELETE_HEAD_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.FIELD_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.LINE_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.NULL_LITERAL;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.QUOTE_CHARACTER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.RETAIN_PREDEFINED_FIELD;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.TIME_FIELD_NAME;

/**
 * Table format factory for providing configured instances of InLongMsgCsv-to-rowdata
 * serializer and deserializer.
 */
public final class InLongMsgCsvFormatFactory implements DeserializationFormatFactory {

    public static final String IDENTIFIER = "inlong-msg-csv";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {

        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateFormatOptions(formatOptions);

        return new InLongMsgCsvDecodingFormat(formatOptions);
    }

    // ------------------------------------------------------------------------
    // Validation
    // ------------------------------------------------------------------------

    static void validateFormatOptions(ReadableConfig tableOptions) {
        // Validate the option value must be a single char.
        validateCharacterVal(tableOptions, FIELD_DELIMITER, true);
        validateCharacterVal(tableOptions, LINE_DELIMITER, true);
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
        options.add(LINE_DELIMITER);
        options.add(QUOTE_CHARACTER);
        options.add(ESCAPE_CHARACTER);
        options.add(NULL_LITERAL);
        options.add(IGNORE_ERRORS);
        options.add(DELETE_HEAD_DELIMITER);
        options.add(RETAIN_PREDEFINED_FIELD);
        return options;
    }
}
