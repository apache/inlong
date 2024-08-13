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

import org.apache.inlong.sort.formats.base.TextFormatDescriptor;
import org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_DELIMITER;

/**
 * Format descriptor for comma-separated values (CSV).
 *
 * <p>This descriptor aims to comply with RFC-4180 ("Common Format and MIME Type
 * for Comma-Separated Values (CSV) Files") proposed by the Internet Engineering
 * Task Force (IETF).
 */
public class InLongMsgTlogCsv extends TextFormatDescriptor<InLongMsgTlogCsv> {

    public static final String FORMAT_TYPE_VALUE = "inlong-msg-tlogcsv";

    public InLongMsgTlogCsv() {
        super(FORMAT_TYPE_VALUE, 1);
    }

    /**
     * Sets the delimiter character (',' by default).
     *
     * @param delimiter the field delimiter character
     */
    public InLongMsgTlogCsv delimiter(char delimiter) {
        internalProperties.putCharacter(FORMAT_DELIMITER, delimiter);
        return this;
    }

    /**
     * Sets the name of the time field.
     *
     * @param timeFieldName The name of the time field.
     */
    public InLongMsgTlogCsv timeFieldName(String timeFieldName) {
        checkNotNull(timeFieldName);
        internalProperties.putString(InLongMsgUtils.FORMAT_TIME_FIELD_NAME, timeFieldName);
        return this;
    }

    /**
     * Sets the name of the attributes field.
     *
     * @param attributesFieldName The name of the attributes field.
     */
    public InLongMsgTlogCsv attributesFieldName(String attributesFieldName) {
        checkNotNull(attributesFieldName);
        internalProperties.putString(InLongMsgUtils.FORMAT_ATTRIBUTES_FIELD_NAME, attributesFieldName);
        return this;
    }
}
