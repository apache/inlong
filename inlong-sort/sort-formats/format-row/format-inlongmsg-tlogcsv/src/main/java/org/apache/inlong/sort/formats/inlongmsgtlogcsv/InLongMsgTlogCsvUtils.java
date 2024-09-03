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

import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;

import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.inlong.sort.formats.base.TableFormatUtils.deserializeBasicField;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.INLONGMSG_ATTR_TIME_DT;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.INLONGMSG_ATTR_TIME_T;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.getPredefinedFields;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.parseAttr;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.parseDateTime;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.parseEpochTime;
import static org.apache.inlong.sort.formats.util.StringUtils.splitCsv;

/**
 * Utilities for {@link InLongMsgTlogCsv}.
 */
public class InLongMsgTlogCsvUtils {

    private static final Logger LOG = LoggerFactory.getLogger(InLongMsgTlogCsvUtils.class);

    public static InLongMsgHead parseHead(String attr) {
        Map<String, String> attributes = parseAttr(attr);

        // Extracts time from the attributes
        Timestamp time;

        if (attributes.containsKey(INLONGMSG_ATTR_TIME_T)) {
            String date = attributes.get(INLONGMSG_ATTR_TIME_T).trim();
            time = parseDateTime(date);
        } else if (attributes.containsKey(INLONGMSG_ATTR_TIME_DT)) {
            String epoch = attributes.get(INLONGMSG_ATTR_TIME_DT).trim();
            time = parseEpochTime(epoch);
        } else {
            throw new IllegalArgumentException(
                    "Could not find " + INLONGMSG_ATTR_TIME_T +
                            " or " + INLONGMSG_ATTR_TIME_DT + " in attributes!");
        }

        // Extracts predefined fields from the attributes
        List<String> predefinedFields = getPredefinedFields(attributes);

        return new InLongMsgHead(attributes, null, time, predefinedFields);
    }

    public static InLongMsgBody parseBody(
            byte[] bytes,
            String charset,
            char delimiter,
            Character escapeChar,
            Character quoteChar,
            boolean isIncludeFirstSegment) {
        String text;
        if (bytes[0] == delimiter) {
            text = new String(bytes, 1, bytes.length - 1, Charset.forName(charset));
        } else {
            text = new String(bytes, Charset.forName(charset));
        }

        String[] segments = splitCsv(text, delimiter, escapeChar, quoteChar);

        String streamId = segments[0];
        List<String> fields =
                Arrays.stream(segments, (isIncludeFirstSegment ? 0 : 1), segments.length).collect(Collectors.toList());

        return new InLongMsgBody(bytes, streamId, fields, Collections.emptyMap());
    }

    /**
     * Deserializes the given fields into the row.
     *
     * @param rowFormatInfo The format information of the row.
     * @param nullLiteral The literal for null values.
     * @param predefinedFields The predefined fields.
     * @param fields The fields.
     * @return The row deserialized from the row.
     */
    public static Row deserializeRow(
            RowFormatInfo rowFormatInfo,
            String nullLiteral,
            List<String> predefinedFields,
            List<String> fields) {
        String[] fieldNames = rowFormatInfo.getFieldNames();
        FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

        int actualNumFields = predefinedFields.size() + fields.size();
        if (actualNumFields != fieldNames.length) {
            LOG.warn("The number of fields mismatches: " + fieldNames.length +
                    " expected, but was " + actualNumFields + ".");
        }

        Row row = new Row(fieldNames.length);

        for (int i = 0; i < predefinedFields.size(); ++i) {

            if (i >= fieldNames.length) {
                break;
            }

            String fieldName = fieldNames[i];
            FormatInfo fieldFormatInfo = fieldFormatInfos[i];

            String fieldText = predefinedFields.get(i);

            Object field =
                    deserializeBasicField(
                            fieldName,
                            fieldFormatInfo,
                            fieldText,
                            nullLiteral);
            row.setField(i, field);
        }

        for (int i = 0; i < fields.size(); ++i) {

            if (i + predefinedFields.size() >= fieldNames.length) {
                break;
            }

            String fieldName = fieldNames[i + predefinedFields.size()];
            FormatInfo fieldFormatInfo = fieldFormatInfos[i + predefinedFields.size()];

            String fieldText = fields.get(i);

            Object field =
                    deserializeBasicField(
                            fieldName,
                            fieldFormatInfo,
                            fieldText,
                            nullLiteral);
            row.setField(i + predefinedFields.size(), field);
        }

        for (int i = predefinedFields.size() + fields.size(); i < fieldNames.length; ++i) {
            row.setField(i, null);
        }

        return row;
    }
}
