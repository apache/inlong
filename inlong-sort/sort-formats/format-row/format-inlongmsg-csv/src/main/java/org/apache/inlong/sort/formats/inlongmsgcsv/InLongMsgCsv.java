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

import org.apache.inlong.sort.formats.base.TextFormatDescriptor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_LINE_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_RETAIN_PREDEFINED_FIELD;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsgcsv.InLongMsgCsvUtils.FORMAT_DELETE_HEAD_DELIMITER;

/**
 * Format descriptor for comma-separated values (CSV).
 *
 * <p>This descriptor aims to comply with RFC-4180 ("Common Format and MIME Type
 * for Comma-Separated Values (CSV) Files") proposed by the Internet Engineering
 * Task Force (IETF).
 */
public class InLongMsgCsv extends TextFormatDescriptor<InLongMsgCsv> {

    public static final String FORMAT_TYPE_VALUE = "inlong-msg-csv";

    public InLongMsgCsv() {
        super(FORMAT_TYPE_VALUE, 1);
    }

    /**
     * Sets the delimiter character (',' by default).
     *
     * @param delimiter the field delimiter character
     */
    public InLongMsgCsv delimiter(char delimiter) {
        internalProperties.putCharacter(FORMAT_DELIMITER, delimiter);
        return this;
    }

    /**
     * Sets the delimiter between lines.
     *
     * @param lineDelimiter The delimiter between lines (e.g. '\n').
     */
    public InLongMsgCsv lineDelimiter(char lineDelimiter) {
        internalProperties.putCharacter(FORMAT_LINE_DELIMITER, lineDelimiter);
        return this;
    }

    /**
     * Retains the delimiter at the first character.
     */
    public InLongMsgCsv retainHeadDelimiter() {
        internalProperties.putBoolean(FORMAT_DELETE_HEAD_DELIMITER, false);
        return this;
    }

    /**
     * Skips the predefined Field.
     */
    public InLongMsgCsv skipPredefinedField() {
        internalProperties.putBoolean(FORMAT_RETAIN_PREDEFINED_FIELD, false);
        return this;
    }

    /**
     * Sets the name of the time field.
     *
     * @param timeFieldName The name of the time field.
     */
    public InLongMsgCsv timeFieldName(String timeFieldName) {
        checkNotNull(timeFieldName);
        internalProperties.putString(FORMAT_TIME_FIELD_NAME, timeFieldName);
        return this;
    }

    /**
     * Sets the name of the attributes field.
     *
     * @param attributesFieldName The name of the attributes field.
     */
    public InLongMsgCsv attributesFieldName(String attributesFieldName) {
        checkNotNull(attributesFieldName);
        internalProperties.putString(FORMAT_ATTRIBUTES_FIELD_NAME, attributesFieldName);
        return this;
    }
}