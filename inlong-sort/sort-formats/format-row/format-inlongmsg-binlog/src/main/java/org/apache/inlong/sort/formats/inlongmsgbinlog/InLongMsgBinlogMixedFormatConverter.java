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

import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.inlongmsg.row.AbstractInLongMsgMixedFormatConverter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Converter used to deserialize a mixed row in tdmsg-dbsync format.
 */
public class InLongMsgBinlogMixedFormatConverter extends AbstractInLongMsgMixedFormatConverter {

    private static final long serialVersionUID = 1L;

    /**
     * The type information of the data fields.
     */
    @Nonnull
    private final RowFormatInfo rowFormatInfo;

    /**
     * The name of the time field.
     */
    @Nullable
    private final String timeFieldName;

    /**
     * The name of the attributes field.
     */
    @Nullable
    private final String attributesFieldName;

    /**
     * The name of the metadata field.
     */
    @Nullable
    private final String metadataFieldName;

    /**
     * Controlling whether beforeColumnsList needs to be included when deserializing dbsync update data.
     */
    private final boolean includeUpdateBefore;

    public InLongMsgBinlogMixedFormatConverter(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nullable String metadataFieldName,
            boolean ignoreErrors) {
        this(
                rowFormatInfo,
                timeFieldName,
                attributesFieldName,
                metadataFieldName,
                ignoreErrors,
                false);
    }

    public InLongMsgBinlogMixedFormatConverter(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nullable String metadataFieldName,
            boolean ignoreErrors,
            boolean includeUpdateBefore) {
        super(ignoreErrors);

        this.rowFormatInfo = rowFormatInfo;
        this.timeFieldName = timeFieldName;
        this.attributesFieldName = attributesFieldName;
        this.metadataFieldName = metadataFieldName;
        this.includeUpdateBefore = includeUpdateBefore;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return InLongMsgBinlogUtils.getRowType(
                rowFormatInfo,
                timeFieldName,
                attributesFieldName,
                metadataFieldName);
    }

    @Override
    public List<Row> convertRows(
            Map<String, String> attributes,
            byte[] data,
            String streamId,
            Timestamp time,
            List<String> predefinedFields,
            List<String> fields,
            Map<String, String> entries) throws Exception {
        return InLongMsgBinlogUtils.getRows(
                rowFormatInfo,
                timeFieldName,
                attributesFieldName,
                metadataFieldName,
                attributes,
                data,
                includeUpdateBefore);
    }

    /**
     * The builder for {@link InLongMsgBinlogMixedFormatConverter}.
     */
    public static class Builder extends InLongMsgBinlogFormatBuilder<Builder> {

        public Builder(RowFormatInfo rowFormatInfo) {
            super(rowFormatInfo);
        }

        public InLongMsgBinlogMixedFormatConverter build() {
            return new InLongMsgBinlogMixedFormatConverter(
                    rowFormatInfo,
                    timeFieldName,
                    attributesFieldName,
                    metadataFieldName,
                    ignoreErrors,
                    includeUpdateBefore);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        if (!super.equals(object)) {
            return false;
        }

        InLongMsgBinlogMixedFormatConverter that = (InLongMsgBinlogMixedFormatConverter) object;
        return rowFormatInfo.equals(that.rowFormatInfo) &&
                Objects.equals(timeFieldName, that.timeFieldName) &&
                Objects.equals(attributesFieldName, that.attributesFieldName) &&
                Objects.equals(metadataFieldName, that.metadataFieldName) &&
                includeUpdateBefore == that.includeUpdateBefore;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rowFormatInfo, timeFieldName, attributesFieldName,
                metadataFieldName, includeUpdateBefore);
    }
}
