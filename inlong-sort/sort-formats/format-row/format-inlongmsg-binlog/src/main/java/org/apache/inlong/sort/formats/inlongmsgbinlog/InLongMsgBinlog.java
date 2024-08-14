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

package org.apache.inlong.sort.formats.inlongmsgbinlog;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatUtils;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.base.FormatDescriptor;

import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_SCHEMA;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsgbinlog.InLongMsgBinlogUtils.FORMAT_METADATA_FIELD_NAME;

/**
 * Format descriptor for InLong binlog.
 *
 */
public class InLongMsgBinlog extends FormatDescriptor {

    public static final String FORMAT_TYPE_VALUE = "inlong-msg-binlog";

    private DescriptorProperties internalProperties = new DescriptorProperties(true);

    private final List<String> fieldNames = new ArrayList<>();
    private final List<FormatInfo> fieldFormatInfos = new ArrayList<>();

    public InLongMsgBinlog() {
        super(FORMAT_TYPE_VALUE, 1);
    }

    /**
     * Ignores the errors in the serialization and deserialization.
     */
    public InLongMsgBinlog ignoreErrors() {
        internalProperties.putBoolean(FORMAT_IGNORE_ERRORS, true);
        return this;
    }

    /**
     * Defines the format for the next field.
     *
     * @param fieldName The name of the field.
     * @param fieldFormatInfo The format of the field.
     */
    public InLongMsgBinlog field(String fieldName, FormatInfo fieldFormatInfo) {
        checkNotNull(fieldName);
        checkNotNull(fieldFormatInfo);

        fieldNames.add(fieldName);
        fieldFormatInfos.add(fieldFormatInfo);

        return this;
    }

    /**
     * Sets the format schema. Required if schema is not derived.
     *
     * <p>Note: This schema defined by this method will be removed if
     * {@link #field(String, FormatInfo)} is called.</p>
     *
     * @param rowFormatInfo The format.
     */
    public InLongMsgBinlog schema(RowFormatInfo rowFormatInfo) {
        checkNotNull(rowFormatInfo);
        internalProperties.putString(FORMAT_SCHEMA, FormatUtils.marshall(rowFormatInfo));
        return this;
    }

    /**
     * Sets the format schema. Required if schema is not derived.
     *
     * @param schema format schema string.
     */
    @Deprecated
    public InLongMsgBinlog schema(String schema) {
        checkNotNull(schema);
        internalProperties.putString(FORMAT_SCHEMA, schema);
        return this;
    }

    /**
     * Sets the name of the time field.
     *
     * @param timeFieldName The name of the time field.
     */
    public InLongMsgBinlog timeFieldName(String timeFieldName) {
        checkNotNull(timeFieldName);
        internalProperties.putString(FORMAT_TIME_FIELD_NAME, timeFieldName);
        return this;
    }

    /**
     * Sets the name of the attributes field.
     *
     * @param attributesFieldName The name of the attributes field.
     */
    public InLongMsgBinlog attributesFieldName(String attributesFieldName) {
        checkNotNull(attributesFieldName);
        internalProperties.putString(FORMAT_ATTRIBUTES_FIELD_NAME, attributesFieldName);
        return this;
    }

    /**
     * Sets the name of the metadata field.
     *
     * @param metadataFieldName The name of the metadata field.
     */
    public InLongMsgBinlog metadataFieldName(String metadataFieldName) {
        checkNotNull(metadataFieldName);
        internalProperties.putString(FORMAT_METADATA_FIELD_NAME, metadataFieldName);
        return this;
    }

    @Override
    protected Map<String, String> toFormatProperties() {
        if (!fieldNames.isEmpty()) {
            RowFormatInfo rowFormatInfo =
                    new RowFormatInfo(
                            fieldNames.toArray(new String[0]),
                            fieldFormatInfos.toArray(new FormatInfo[0]));

            String schema = FormatUtils.marshall(rowFormatInfo);
            internalProperties.putString(FORMAT_SCHEMA, schema);
        }

        return internalProperties.asMap();
    }
}
