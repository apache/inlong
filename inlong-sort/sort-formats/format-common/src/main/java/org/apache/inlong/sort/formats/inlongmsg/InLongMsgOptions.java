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

package org.apache.inlong.sort.formats.inlongmsg;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELETE_HEAD_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_INCLUDE_UPDATE_BEFORE;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_METADATA_FIELD_NAME;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_RETAIN_PREDEFINED_FIELD;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ATTRIBUTE_FIELD_NAME;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_DELETE_HEAD_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_FIELD_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_INCLUDE_UPDATE_BEFORE;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_LINE_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_METADATA_FIELD_NAME;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_QUOTE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_RETAIN_PREDEFINED_FIELD;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_TIME_FIELD_NAME;

public class InLongMsgOptions {

    private InLongMsgOptions() {
    }

    public static final ConfigOption<String> INNER_FORMAT =
            ConfigOptions.key("inner.format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Defines the format identifier for encoding attr data. \n"
                            + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Optional flag to skip fields and rows with parse errors instead of failing;\n"
                            + "fields are set to null in case of errors");

    public static void validateDecodingFormatOptions(ReadableConfig config) {
        String innerFormat = config.get(INNER_FORMAT);
        if (innerFormat == null) {
            throw new ValidationException(
                    INNER_FORMAT.key() + " shouldn't be null.");
        }
    }

    public static final ConfigOption<String> TIME_FIELD_NAME =
            ConfigOptions.key(FORMAT_TIME_FIELD_NAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the time field in InLongMsg.");

    public static final ConfigOption<String> ATTRIBUTE_FIELD_NAME =
            ConfigOptions.key(FORMAT_ATTRIBUTE_FIELD_NAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the attribute field in InLongMsg)");

    public static final ConfigOption<String> CHARSET =
            ConfigOptions.key(FORMAT_CHARSET)
                    .stringType()
                    .defaultValue(DEFAULT_CHARSET)
                    .withDescription("Optional text encoding format ('utf-8' by default)");

    public static final ConfigOption<String> FIELD_DELIMITER =
            ConfigOptions.key(FORMAT_FIELD_DELIMITER)
                    .stringType()
                    .defaultValue(String.valueOf(DEFAULT_DELIMITER))
                    .withDescription("Optional field delimiter character (',' by default)");

    public static final ConfigOption<String> LINE_DELIMITER =
            ConfigOptions.key(FORMAT_LINE_DELIMITER)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional line delimiter character");

    public static final ConfigOption<String> ESCAPE_CHARACTER =
            ConfigOptions.key(FORMAT_ESCAPE_CHARACTER)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional escape character for escaping values (disabled by default)");

    public static final ConfigOption<String> QUOTE_CHARACTER =
            ConfigOptions.key(FORMAT_QUOTE_CHARACTER)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional quote character for enclosing field values (disabled by default)");

    public static final ConfigOption<String> NULL_LITERAL =
            ConfigOptions.key(FORMAT_NULL_LITERAL)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional null literal string that is interpreted as a\n"
                                    + "null value (disabled by default)");

    public static final ConfigOption<Boolean> IGNORE_ERRORS =
            ConfigOptions.key(FORMAT_IGNORE_ERRORS)
                    .booleanType()
                    .defaultValue(DEFAULT_IGNORE_ERRORS)
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors");

    public static final ConfigOption<Boolean> DELETE_HEAD_DELIMITER =
            ConfigOptions.key(FORMAT_DELETE_HEAD_DELIMITER)
                    .booleanType()
                    .defaultValue(DEFAULT_DELETE_HEAD_DELIMITER)
                    .withDescription(
                            "True if the head delimiter should be removed (false by default).");

    public static final ConfigOption<Boolean> RETAIN_PREDEFINED_FIELD =
            ConfigOptions.key(FORMAT_RETAIN_PREDEFINED_FIELD)
                    .booleanType()
                    .defaultValue(DEFAULT_RETAIN_PREDEFINED_FIELD)
                    .withDescription(
                            "True if the retain predefined field should be skip the predefined Field. (true by default).");

    public static final ConfigOption<String> METADATA_FIELD_NAME =
            ConfigOptions.key(FORMAT_METADATA_FIELD_NAME)
                    .stringType()
                    .defaultValue(DEFAULT_METADATA_FIELD_NAME)
                    .withDescription(
                            "True if the retain predefined field should be skip the predefined Field. (true by default).");

    public static final ConfigOption<Boolean> INCLUDE_UPDATE_BEFORE =
            ConfigOptions.key(FORMAT_INCLUDE_UPDATE_BEFORE)
                    .booleanType()
                    .defaultValue(DEFAULT_INCLUDE_UPDATE_BEFORE)
                    .withDescription(
                            "True if the retain predefined field should be skip the predefined Field. (true by default).");

    public static final ConfigOption<Boolean> CSV_IGNORE_PARSE_ERRORS =
            ConfigOptions.key("csv.ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Allows the case that real size exceeds the expected size.\n "
                            + "The extra column will be skipped");

    public static final ConfigOption<Boolean> CSV_IGNORE_TRAILING_UNMAPPABLE =
            ConfigOptions.key("csv.ignore-trailing-unmappable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Allows the case that real size exceeds the expected size.\n "
                            + "The extra column will be skipped");

    public static final ConfigOption<Boolean> CSV_INSERT_NULLS_FOR_MISSING_COLUMNS =
            ConfigOptions.key("csv.insert-nulls-for-missing-columns")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("For missing columns, insert null.");

    public static final ConfigOption<Boolean> CSV_EMPTY_STRING_AS_NULL =
            ConfigOptions.key("csv.empty-string-as-null")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("if the string value is empty, make it as null");

}
