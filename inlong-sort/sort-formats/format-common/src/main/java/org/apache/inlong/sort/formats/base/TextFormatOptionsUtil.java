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

import org.apache.inlong.sort.formats.base.TextFormatOptions.MapNullKeyMode;
import org.apache.inlong.sort.formats.base.TextFormatOptions.TimestampFormat;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.inlong.sort.formats.base.TableFormatOptions.IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.CHARSET;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.FAIL_ON_MISSING_FIELD;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.MAP_NULL_KEY_MODE;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.TIMESTAMP_FORMAT;

/** Utils for {@link TextFormatOptions}. */
public class TextFormatOptionsUtil {

    // --------------------------------------------------------------------------------------------
    // Option enumerations
    // --------------------------------------------------------------------------------------------

    public static final String SQL = "SQL";
    public static final String ISO_8601 = "ISO-8601";

    public static final Set<String> TIMESTAMP_FORMAT_ENUM = new HashSet<>(Arrays.asList(SQL, ISO_8601));

    // The handling mode of null key for map data
    public static final String MAP_NULL_KEY_MODE_FAIL = "FAIL";
    public static final String MAP_NULL_KEY_MODE_DROP = "DROP";
    public static final String MAP_NULL_KEY_MODE_LITERAL = "LITERAL";

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static TimestampFormat getTimestampFormat(String timestampFormat) {
        switch (timestampFormat) {
            case SQL:
                return TimestampFormat.SQL;
            case ISO_8601:
                return TimestampFormat.ISO_8601;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
    }

    /**
     * Creates handling mode for null key map data.
     *
     * <p>See {@link #MAP_NULL_KEY_MODE_FAIL}, {@link #MAP_NULL_KEY_MODE_DROP}, and {@link
     * #MAP_NULL_KEY_MODE_LITERAL} for more information.
     */
    public static MapNullKeyMode getMapNullKeyMode(String mapNullKeyMode) {
        switch (mapNullKeyMode.toUpperCase()) {
            case MAP_NULL_KEY_MODE_FAIL:
                return MapNullKeyMode.FAIL;
            case MAP_NULL_KEY_MODE_DROP:
                return MapNullKeyMode.DROP;
            case MAP_NULL_KEY_MODE_LITERAL:
                return MapNullKeyMode.LITERAL;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported map null key handling mode '%s'. Validator should have checked that.",
                                mapNullKeyMode));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    /** Validator for text decoding format. */
    public static void validateDecodingFormatOptions(ReadableConfig tableOptions) {
        boolean failOnMissingField = tableOptions.get(FAIL_ON_MISSING_FIELD);
        boolean ignoreParseErrors = tableOptions.get(IGNORE_ERRORS);
        if (ignoreParseErrors && failOnMissingField) {
            throw new ValidationException(
                    String.format("%s and %s shouldn't both be true.",
                            FAIL_ON_MISSING_FIELD.key(),
                            IGNORE_ERRORS.key()));
        }
        validateTimestampFormat(tableOptions);
        validateCharset(tableOptions);
    }

    /** Validator for text encoding format. */
    public static void validateEncodingFormatOptions(ReadableConfig tableOptions) {
        Set<String> nullKeyModes =
                Arrays.stream(MapNullKeyMode.values())
                        .map(Objects::toString)
                        .collect(Collectors.toSet());
        if (!nullKeyModes.contains(tableOptions.get(MAP_NULL_KEY_MODE).toUpperCase())) {
            throw new ValidationException(
                    String.format(
                            "Unsupported value '%s' for option %s. Supported values are %s.",
                            tableOptions.get(MAP_NULL_KEY_MODE),
                            MAP_NULL_KEY_MODE.key(),
                            nullKeyModes));
        }
        validateTimestampFormat(tableOptions);
        validateCharset(tableOptions);
    }

    /** Validates timestamp format which value should be SQL or ISO-8601. */
    static void validateTimestampFormat(ReadableConfig tableOptions) {
        String timestampFormat = tableOptions.get(TIMESTAMP_FORMAT);
        if (!TIMESTAMP_FORMAT_ENUM.contains(timestampFormat)) {
            throw new ValidationException(
                    String.format(
                            "Unsupported value '%s' for %s. Supported values are [SQL, ISO-8601].",
                            timestampFormat, TIMESTAMP_FORMAT.key()));
        }
    }

    public static void validateCharset(ReadableConfig tableOptions) {
        String charset = tableOptions.get(CHARSET);
        try {
            Charset.forName(charset);
        } catch (Exception e) {
            throw new ValidationException(String.format("Charset %s is not supported.", charset));
        }
    }

    private TextFormatOptionsUtil() {
    }
}
