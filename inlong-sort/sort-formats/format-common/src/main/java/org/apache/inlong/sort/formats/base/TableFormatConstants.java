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

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * Common constants used in various formats.
 */
public class TableFormatConstants {

    public static final String FORMAT_SCHEMA = "format.schema";
    public static final String FORMAT_DELIMITER = "format.delimiter";
    public static final String FORMAT_DERIVE_SCHEMA = "format.derive-schema";
    public static final String FORMAT_ENTRY_DELIMITER = "format.entry-delimiter";
    public static final String FORMAT_KV_DELIMITER = "format.kv-delimiter";
    public static final String FORMAT_LINE_DELIMITER = "format.line-delimiter";
    public static final String FORMAT_NULL_LITERAL = "format.null-literal";
    public static final String FORMAT_ESCAPE_CHARACTER = "format.escape-character";
    public static final String FORMAT_QUOTE_CHARACTER = "format.quote-character";
    public static final String FORMAT_IGNORE_ERRORS = "format.ignore-errors";
    public static final String FORMAT_CHARSET = "format.charset";
    public static final String FORMAT_TYPE = "format.type";
    public static final String FORMAT_PROPERTY_VERSION = "format.property-version";
    public static final String FORMAT_FIELD_DELIMITER = "format.field-delimiter";
    public static final String FORMAT_TIME_FIELD_NAME = "format.time-field-name";
    public static final String FORMAT_KV_ENTRY_DELIMITER = "format.entry-delimiter";
    public static final String FORMAT_ATTRIBUTE_FIELD_NAME = "format.attribute-field-name";
    public static final String FORMAT_IS_MIXED = "format.is-mixed";
    public static final String FORMAT_DELETE_HEAD_DELIMITER = "format.delete-head-delimiter";
    public static final String FORMAT_RETAIN_PREDEFINED_FIELD = "format.retain-predefined-field";
    public static final String FORMAT_METADATA_FIELD_NAME = "format.metadata-field-name";
    public static final String FORMAT_INCLUDE_UPDATE_BEFORE = "format.include-update-before";
    public static final String FORMAT_PROPERTIES = "properties";

    public static final char DEFAULT_DELIMITER = ',';
    public static final char DEFAULT_ENTRY_DELIMITER = '&';
    public static final char DEFAULT_KV_DELIMITER = '=';
    public static final Character DEFAULT_LINE_DELIMITER = null;
    public static final Character DEFAULT_ESCAPE_CHARACTER = null;
    public static final Character DEFAULT_QUOTE_CHARACTER = null;
    public static final String DEFAULT_NULL_LITERAL = null;
    public static final boolean DEFAULT_IGNORE_ERRORS = false;
    public static final String DEFAULT_CHARSET = "UTF-8";

    public static final boolean DEFAULT_IS_MIXED = false;
    public static final boolean DEFAULT_DELETE_HEAD_DELIMITER = false;
    public static final boolean DEFAULT_RETAIN_PREDEFINED_FIELD = true;
    public static final int DEFAULT_BYTE_ARRAY_STREAM_LENGTH = 2048;
    public static final String DEFAULT_METADATA_FIELD_NAME = "inlongmsg_metadata";

    public static final boolean DEFAULT_INCLUDE_UPDATE_BEFORE = false;

    public static final DateTimeFormatter SQL_TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    public static final DateTimeFormatter SQL_TIMESTAMP_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(SQL_TIME_FORMAT)
                    .toFormatter();

    public static final DateTimeFormatter ISO8601_TIMESTAMP_FORMAT =
            DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    public static final DateTimeFormatter SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(SQL_TIME_FORMAT)
                    .appendPattern("'Z'")
                    .toFormatter();

    public static final DateTimeFormatter ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral('T')
                    .append(DateTimeFormatter.ISO_LOCAL_TIME)
                    .appendPattern("'Z'")
                    .toFormatter();
}
