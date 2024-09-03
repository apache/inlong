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

package org.apache.inlong.sort.formats.inlongmsgtlogkv;

import org.apache.inlong.sort.formats.base.TextFormatDescriptor;
import org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_KV_DELIMITER;

/**
 * Format descriptor for InLongMsgTlogKVs.
 */
public class InLongMsgTlogKv extends TextFormatDescriptor<InLongMsgTlogKv> {

    public static final String FORMAT_TYPE_VALUE = "inlong-msg-tlogkv";

    public InLongMsgTlogKv() {
        super(FORMAT_TYPE_VALUE, 1);
    }

    /**
     * Sets the delimiter character (',' by default).
     *
     * @param delimiter the field delimiter character.
     * @return The instance which has delimiter set.
     */
    public InLongMsgTlogKv delimiter(char delimiter) {
        internalProperties.putCharacter(FORMAT_DELIMITER, delimiter);
        return this;
    }

    /**
     * Sets the entry delimiter character ('&amp;' by default).
     *
     * @param delimiter the entry delimiter character.
     * @return The instance which has delimiter set.
     */
    public InLongMsgTlogKv entryDelimiter(char delimiter) {
        internalProperties.putCharacter(FORMAT_ENTRY_DELIMITER, delimiter);
        return this;
    }

    /**
     * Sets the kv delimiter character ('=' by default).
     *
     * @param delimiter the kv delimiter character.
     * @return The instance which has delimiter set.
     */
    public InLongMsgTlogKv kvDelimiter(char delimiter) {
        internalProperties.putCharacter(FORMAT_KV_DELIMITER, delimiter);
        return this;
    }

    /**
     * Sets the name of the time field.
     *
     * @param timeFieldName The name of the time field.
     * @return The instance which has time field name set.
     */
    public InLongMsgTlogKv timeFieldName(String timeFieldName) {
        checkNotNull(timeFieldName);
        internalProperties.putString(InLongMsgUtils.FORMAT_TIME_FIELD_NAME, timeFieldName);
        return this;
    }

    /**
     * Sets the name of the attributes field.
     *
     * @param attributesFieldName The name of the attributes field.
     * @return The instance which has attributes field name set.
     */
    public InLongMsgTlogKv attributesFieldName(String attributesFieldName) {
        checkNotNull(attributesFieldName);
        internalProperties.putString(InLongMsgUtils.FORMAT_ATTRIBUTES_FIELD_NAME, attributesFieldName);
        return this;
    }
}
