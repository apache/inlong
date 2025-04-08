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

package org.apache.inlong.sort.formats.inlongmsgkv;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;

import org.apache.flink.types.Row;

import java.nio.charset.Charset;
import java.sql.Timestamp;
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
import static org.apache.inlong.sort.formats.util.StringUtils.splitKv;

/**
 * Utilities for {@link InLongMsgKv}.
 */
public class InLongMsgKvUtils {

    public static final String DEFAULT_INLONGMSGKV_CHARSET = "ISO_8859_1";

    public static InLongMsgHead parseHead(String attr) {
        Map<String, String> attributes = parseAttr(attr);

        String streamId;
        if (attributes.containsKey(INLONGMSG_ATTR_STREAM_ID)) {
            streamId = attributes.get(INLONGMSG_ATTR_STREAM_ID);
        } else if (attributes.containsKey(INLONGMSG_ATTR_INTERFACE_NAME)) {
            streamId = attributes.get(INLONGMSG_ATTR_INTERFACE_NAME);
        } else if (attributes.containsKey(INLONGMSG_ATTR_TID)) {
            streamId = attributes.get(INLONGMSG_ATTR_TID);
        } else {
            throw new IllegalArgumentException(
                    "Could not find " + INLONGMSG_ATTR_STREAM_ID +
                            " or " + INLONGMSG_ATTR_TID + " in attributes!");
        }

        Timestamp time;
        if (attributes.containsKey(INLONGMSG_ATTR_TIME_DT)) {
            String epoch = attributes.get(INLONGMSG_ATTR_TIME_DT).trim();
            time = parseEpochTime(epoch);
        } else if (attributes.containsKey(INLONGMSG_ATTR_TIME_T)) {
            String date = attributes.get(INLONGMSG_ATTR_TIME_T).trim();
            time = parseDateTime(date);
        } else {
            throw new IllegalArgumentException(
                    "Could not find " + INLONGMSG_ATTR_TIME_T +
                            " or " + INLONGMSG_ATTR_TIME_T + " in attributes!");
        }

        List<String> predefinedFields = getPredefinedFields(attributes);

        return new InLongMsgHead(attributes, streamId, time, predefinedFields);
    }

    public static List<InLongMsgBody> parseBodyList(
            byte[] bytes,
            String charset,
            char entryDelimiter,
            char kvDelimiter,
            Character lineDelimiter,
            Character escapeChar,
            Character quoteChar) {
        String text = new String(bytes, Charset.forName(charset));

        List<Map<String, String>> list =
                splitKv(
                        text,
                        entryDelimiter,
                        kvDelimiter,
                        escapeChar,
                        quoteChar,
                        lineDelimiter, true);

        return list.stream().map((line) -> {
            return new InLongMsgBody(
                    bytes,
                    null,
                    Collections.emptyList(),
                    line);
        }).collect(Collectors.toList());
    }

    /**
     * Deserializes the row from the given entries.
     *
     * @param rowFormatInfo The format of the fields.
     * @param nullLiteral The literal for null values.
     * @param predefinedFields The predefined fields.
     * @param entries The entries.
     * @return The row deserialized from the given entries.
     */
    public static Row deserializeRow(
            RowFormatInfo rowFormatInfo,
            String nullLiteral,
            List<String> predefinedFields,
            Map<String, String> entries) throws Exception {
        String[] fieldNames = rowFormatInfo.getFieldNames();
        FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

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

        for (int i = predefinedFields.size(); i < fieldNames.length; ++i) {

            String fieldName = fieldNames[i];
            FormatInfo fieldFormatInfo = fieldFormatInfos[i];

            String fieldText = entries.get(fieldName);

            Object field =
                    deserializeBasicField(
                            fieldName,
                            fieldFormatInfo,
                            fieldText,
                            nullLiteral, null);
            row.setField(i, field);
        }

        return row;
    }
}
