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
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.INLONGMSG_ATTR_INTERFACE_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.INLONGMSG_ATTR_STREAM_ID;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.INLONGMSG_ATTR_TID;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.INLONGMSG_ATTR_TIME_DT;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.INLONGMSG_ATTR_TIME_T;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.getPredefinedFields;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.parseAttr;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.parseDateTime;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.parseEpochTime;
import static org.apache.inlong.sort.formats.util.StringUtils.splitCsv;

/**
 * Utilities for {@link InLongMsgCsv}.
 */
public class InLongMsgCsvUtils {

    private static final Logger LOG = LoggerFactory.getLogger(InLongMsgCsvUtils.class);

    public static final String FORMAT_DELETE_HEAD_DELIMITER = "format.delete-head-delimiter";
    public static final boolean DEFAULT_DELETE_HEAD_DELIMITER = true;

    public static InLongMsgHead parseHead(String attr) {
        Map<String, String> attributes = parseAttr(attr);

        // Extracts interface from the attributes.
        String streamId;

        if (attributes.containsKey(INLONGMSG_ATTR_STREAM_ID)) {
            streamId = attributes.get(INLONGMSG_ATTR_STREAM_ID);
        } else if (attributes.containsKey(INLONGMSG_ATTR_TID)) {
            streamId = attributes.get(INLONGMSG_ATTR_TID);
        } else if (attributes.containsKey(INLONGMSG_ATTR_INTERFACE_NAME)) {
            streamId = attributes.get(INLONGMSG_ATTR_INTERFACE_NAME);
        } else {
            throw new IllegalArgumentException(
                    "Could not find " + INLONGMSG_ATTR_STREAM_ID
                            + " or " + INLONGMSG_ATTR_TID
                            + " or " + INLONGMSG_ATTR_INTERFACE_NAME
                            + " in attributes!");
        }

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
                    "Could not find " + INLONGMSG_ATTR_TIME_T
                            + " or " + INLONGMSG_ATTR_TIME_DT + " in attributes!");
        }

        // Extracts predefined fields from the attributes
        List<String> predefinedFields = getPredefinedFields(attributes);

        return new InLongMsgHead(attributes, streamId, time, predefinedFields);
    }

    public static List<InLongMsgBody> parseBodyList(
            byte[] bytes,
            String charset,
            char delimiter,
            Character lineDelimiter,
            Character escapeChar,
            Character quoteChar,
            boolean deleteHeadDelimiter) {
        String bodyStr = new String(bytes, Charset.forName(charset));

        String[][] split =
                splitCsv(bodyStr, delimiter, escapeChar, quoteChar, lineDelimiter, deleteHeadDelimiter,
                        true);

        return Arrays.stream(split)
                .map((line) -> {
                    // Only parsed fields will be used by downstream, so it's safe to leave
                    // the other parameters empty.
                    return new InLongMsgBody(
                            null,
                            null,
                            Arrays.asList(line),
                            Collections.emptyMap());
                }).collect(Collectors.toList());
    }

    /**
     * Deserializes the given fields into row.
     *
     * @param rowFormatInfo The format of the row.
     * @param nullLiteral The literal for null values.
     * @param predefinedFields The text of predefined fields.
     * @param fields The texts of the fields.
     * @return The row deserialized from the given field texts.
     */
    public static Row deserializeRow(
            RowFormatInfo rowFormatInfo,
            String nullLiteral,
            List<String> predefinedFields,
            List<String> fields) throws Exception {
        String[] fieldNames = rowFormatInfo.getFieldNames();
        FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

        int actualNumFields = predefinedFields.size() + fields.size();
        if (actualNumFields != fieldNames.length) {
            LOG.warn("The number of fields mismatches: expected={}, actual={}. "
                    + "PredefinedFields=[{}], Fields=[{}]", fieldNames.length, actualNumFields,
                    predefinedFields, fields);
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
                            nullLiteral, null);
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
                            nullLiteral, null);
            row.setField(i + predefinedFields.size(), field);
        }
        for (int i = predefinedFields.size() + fields.size(); i < fieldNames.length; ++i) {
            row.setField(i, null);
        }
        return row;
    }
}