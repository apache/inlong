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

package org.apache.inlong.sort.formats.base;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_QUOTE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_FIELD_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_QUOTE_CHARACTER;

/**
 * Text format options.
 */
@PublicEvolving
public class TextFormatOptions {

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

    public static final ConfigOption<String> QUOTE_CHARACTER =
            ConfigOptions.key(FORMAT_QUOTE_CHARACTER)
                    .stringType()
                    .defaultValue(String.valueOf(DEFAULT_QUOTE_CHARACTER))
                    .withDescription(
                            "Optional quote character for enclosing field values ('\"' by default)");

    public static final ConfigOption<String> ESCAPE_CHARACTER =
            ConfigOptions.key(FORMAT_ESCAPE_CHARACTER)
                    .stringType()
                    .defaultValue(String.valueOf(DEFAULT_ESCAPE_CHARACTER))
                    .withDescription(
                            "Optional escape character for escaping values (disabled by default)");

    public static final ConfigOption<String> NULL_LITERAL =
            ConfigOptions.key(FORMAT_NULL_LITERAL)
                    .stringType()
                    .defaultValue(FORMAT_NULL_LITERAL)
                    .withDescription(
                            "Optional null literal string that is interpreted as a\n"
                                    + "null value (disabled by default)");

    public static final ConfigOption<String> KV_ENTRY_DELIMITER =
            ConfigOptions.key(TableFormatConstants.FORMAT_KV_ENTRY_DELIMITER)
                    .stringType()
                    .defaultValue(String.valueOf(TableFormatConstants.DEFAULT_ENTRY_DELIMITER));

    public static final ConfigOption<String> KV_DELIMITER =
            ConfigOptions.key(TableFormatConstants.FORMAT_KV_DELIMITER)
                    .stringType()
                    .defaultValue(String.valueOf(TableFormatConstants.DEFAULT_KV_DELIMITER));

    public static final ConfigOption<String> LINE_DELIMITER =
            ConfigOptions.key(TableFormatConstants.FORMAT_LINE_DELIMITER)
                    .stringType()
                    .defaultValue(String.valueOf(TableFormatConstants.DEFAULT_LINE_DELIMITER));

    public static final ConfigOption<String> FORMAT_PROPERTIES =
            ConfigOptions.key(TableFormatConstants.FORMAT_PROPERTIES)
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> MAP_NULL_KEY_MODE =
            ConfigOptions.key("map-null-key.mode")
                    .stringType()
                    .defaultValue("FAIL")
                    .withDescription(
                            "Optional flag to control the handling mode when serializing null key for map data, FAIL by default."
                                    + " Option DROP will drop null key entries for map data."
                                    + " Option LITERAL will use 'map-null-key.literal' as key literal.");

    public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD =
            ConfigOptions.key("fail-on-missing-field")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to specify whether to fail if a field is missing or not, false by default.");

    public static final ConfigOption<String> MAP_NULL_KEY_LITERAL =
            ConfigOptions.key("map-null-key.literal")
                    .stringType()
                    .defaultValue("null")
                    .withDescription(
                            "Optional flag to specify string literal for null keys when 'map-null-key.mode' is LITERAL, \"null\" by default.");

    public static final ConfigOption<String> TIMESTAMP_FORMAT =
            ConfigOptions.key("timestamp-format.standard")
                    .stringType()
                    .defaultValue("SQL")
                    .withDescription(
                            "Optional flag to specify timestamp format, SQL by default."
                                    + " Option ISO-8601 will parse input timestamp in \"yyyy-MM-ddTHH:mm:ss.s{precision}\" format and output timestamp in the same format."
                                    + " Option SQL will parse input timestamp in \"yyyy-MM-dd HH:mm:ss.s{precision}\" format and output timestamp in the same format.");

    /** Handling mode for map data with null key. */
    public enum MapNullKeyMode {
        FAIL,
        DROP,
        LITERAL
    }

    /** Timestamp format Enums. */
    public enum TimestampFormat {
        /**
         * Options to specify TIMESTAMP/TIMESTAMP_WITH_LOCAL_ZONE format. It will parse TIMESTAMP in
         * "yyyy-MM-dd HH:mm:ss.s{precision}" format, TIMESTAMP_WITH_LOCAL_TIMEZONE in "yyyy-MM-dd
         * HH:mm:ss.s{precision}'Z'" and output in the same format.
         */
        SQL,

        /**
         * Options to specify TIMESTAMP/TIMESTAMP_WITH_LOCAL_ZONE format. It will parse TIMESTAMP in
         * "yyyy-MM-ddTHH:mm:ss.s{precision}" format, TIMESTAMP_WITH_LOCAL_TIMEZONE in
         * "yyyy-MM-ddTHH:mm:ss.s{precision}'Z'" and output in the same format.
         */
        ISO_8601
    }

    private TextFormatOptions() {
    }
}
