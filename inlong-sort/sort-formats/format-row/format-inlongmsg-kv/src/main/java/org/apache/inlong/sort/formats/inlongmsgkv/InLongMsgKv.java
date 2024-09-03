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

import org.apache.inlong.sort.formats.base.TextFormatDescriptor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_LINE_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_RETAIN_PREDEFINED_FIELD;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_TIME_FIELD_NAME;

/**
 * Format descriptor for KVs.
 */
public class InLongMsgKv extends TextFormatDescriptor<InLongMsgKv> {

    public static final String FORMAT_TYPE_VALUE = "inlong-msg-kv";

    public InLongMsgKv() {
        super(FORMAT_TYPE_VALUE, 1);
    }

    /**
     * Sets the entry delimiter character ('&amp;' by default).
     *
     * @param delimiter the entry delimiter character
     * @return The instance which has delimiter set.
     */
    public InLongMsgKv entryDelimiter(char delimiter) {
        internalProperties.putCharacter(FORMAT_ENTRY_DELIMITER, delimiter);
        return this;
    }

    /**
     * Sets the kv delimiter character ('=' by default).
     *
     * @param delimiter the kv delimiter character.
     * @return The instance which has delimiter set.
     */
    public InLongMsgKv kvDelimiter(char delimiter) {
        internalProperties.putCharacter(FORMAT_KV_DELIMITER, delimiter);
        return this;
    }

    /**
     * Sets the delimiter between lines.
     *
     * @param lineDelimiter The delimiter between lines (e.g. '\n').
     * @return The instance which has lineDelimiter set.
     */
    public InLongMsgKv lineDelimiter(char lineDelimiter) {
        internalProperties.putCharacter(FORMAT_LINE_DELIMITER, lineDelimiter);
        return this;
    }

    /**
     * Sets the name of the time field.
     *
     * @param timeFieldName The name of the time field.
     * @return The instance which has timeFieldName set.
     */
    public InLongMsgKv timeFieldName(String timeFieldName) {
        checkNotNull(timeFieldName);
        internalProperties.putString(FORMAT_TIME_FIELD_NAME, timeFieldName);
        return this;
    }

    /**
     * Sets the name of the attributes field.
     *
     * @param attributesFieldName The name of the attributes field.
     * @return The instance which has attributes field set.
     */
    public InLongMsgKv attributesFieldName(String attributesFieldName) {
        checkNotNull(attributesFieldName);
        internalProperties.putString(FORMAT_ATTRIBUTES_FIELD_NAME, attributesFieldName);
        return this;
    }

    /**
     * Skips the predefined Field.
     */
    public InLongMsgKv skipPredefinedField() {
        internalProperties.putBoolean(FORMAT_RETAIN_PREDEFINED_FIELD, false);
        return this;
    }
}
