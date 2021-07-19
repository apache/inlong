/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.formats.tdmsgcsv;

import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.TDMSG_ATTR_INTERFACE_ID;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.TDMSG_ATTR_INTERFACE_NAME;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.TDMSG_ATTR_INTERFACE_TID;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.TDMSG_ATTR_TIME_DT;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.TDMSG_ATTR_TIME_T;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.getPredefinedFields;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.parseAttr;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.parseDateTime;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.parseEpochTime;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.base.TableFormatUtils;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.tdmsg.TDMsgBody;
import org.apache.inlong.sort.formats.tdmsg.TDMsgHead;
import org.apache.inlong.sort.formats.tdmsg.TDMsgUtils;
import org.apache.inlong.sort.formats.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for {@link TDMsgCsv}.
 */
public class TDMsgCsvUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TDMsgUtils.class);

    public static final String FORMAT_DELETE_HEAD_DELIMITER = "format.delete-head-delimiter";
    public static final boolean DEFAULT_DELETE_HEAD_DELIMITER = true;

    public static TDMsgHead parseHead(String attr) {
        Map<String, String> attributes = parseAttr(attr);

        // Extracts interface from the attributes.
        String tid;

        if (attributes.containsKey(TDMSG_ATTR_INTERFACE_NAME)) {
            tid = attributes.get(TDMSG_ATTR_INTERFACE_NAME);
        } else if (attributes.containsKey(TDMSG_ATTR_INTERFACE_ID)) {
            tid = attributes.get(TDMSG_ATTR_INTERFACE_ID);
        } else if (attributes.containsKey(TDMSG_ATTR_INTERFACE_TID)) {
            tid = attributes.get(TDMSG_ATTR_INTERFACE_TID);
        } else {
            throw new IllegalArgumentException(
                    "Could not find " + TDMSG_ATTR_INTERFACE_NAME
                            + " or " + TDMSG_ATTR_INTERFACE_ID
                            + " or " + TDMSG_ATTR_INTERFACE_TID + " in attributes!");
        }

        // Extracts time from the attributes
        Timestamp time;

        if (attributes.containsKey(TDMSG_ATTR_TIME_T)) {
            String date = attributes.get(TDMSG_ATTR_TIME_T).trim();
            time = parseDateTime(date);
        } else if (attributes.containsKey(TDMSG_ATTR_TIME_DT)) {
            String epoch = attributes.get(TDMSG_ATTR_TIME_DT).trim();
            time = parseEpochTime(epoch);
        } else {
            throw new IllegalArgumentException(
                    "Could not find " + TDMSG_ATTR_TIME_T
                            + " or " + TDMSG_ATTR_TIME_DT + " in attributes!");
        }

        // Extracts predefined fields from the attributes
        List<String> predefinedFields = getPredefinedFields(attributes);

        return new TDMsgHead(attributes, tid, time, predefinedFields);
    }

    public static TDMsgBody parseBody(
            byte[] bytes,
            String charset,
            char delimiter,
            Character escapeChar,
            Character quoteChar,
            boolean deleteHeadDelimiter
    ) {

        String bodyText;
        if (bytes[0] == delimiter && deleteHeadDelimiter) {
            bodyText = new String(bytes, 1, bytes.length - 1, Charset.forName(charset));
        } else {
            bodyText = new String(bytes, Charset.forName(charset));
        }

        String[] fieldTexts = StringUtils.splitCsv(bodyText, delimiter, escapeChar, quoteChar);

        return new TDMsgBody(
                bytes,
                null,
                Arrays.asList(fieldTexts),
                Collections.emptyMap()
        );
    }

    public static Row buildRow(
            RowFormatInfo rowFormatInfo,
            String nullLiteral,
            Timestamp time,
            Map<String, String> attributes,
            List<String> predefinedFields,
            List<String> fields
    ) {
        String[] fieldNames = rowFormatInfo.getFieldNames();
        FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

        int actualNumFields = predefinedFields.size() + fields.size();
        if (actualNumFields != fieldNames.length) {
            LOG.warn("The number of fields mismatches: " + fieldNames.length
                     + " expected, but was " + actualNumFields + ".");
        }

        Row row = new Row(2 + fieldNames.length);
        row.setField(0, time);
        row.setField(1, attributes);

        for (int i = 0; i < predefinedFields.size(); ++i) {

            if (i >= fieldNames.length) {
                break;
            }

            String fieldName = fieldNames[i];
            FormatInfo fieldFormatInfo = fieldFormatInfos[i];

            String fieldText = predefinedFields.get(i);

            Object field =
                    TableFormatUtils.deserializeBasicField(
                            fieldName,
                            fieldFormatInfo,
                            fieldText,
                            nullLiteral
                    );
            row.setField(i + 2, field);
        }

        for (int i = 0; i < fields.size(); ++i) {

            if (i + predefinedFields.size() >= fieldNames.length) {
                break;
            }

            String fieldName = fieldNames[i + predefinedFields.size()];
            FormatInfo fieldFormatInfo = fieldFormatInfos[i + predefinedFields.size()];

            String fieldText = fields.get(i);

            Object field =
                    TableFormatUtils.deserializeBasicField(
                            fieldName,
                            fieldFormatInfo,
                            fieldText,
                            nullLiteral
                    );
            row.setField(i + predefinedFields.size() + 2, field);
        }

        for (int i = predefinedFields.size() + fields.size(); i < fieldNames.length; ++i) {
            row.setField(i + 2, null);
        }

        return row;
    }
}
