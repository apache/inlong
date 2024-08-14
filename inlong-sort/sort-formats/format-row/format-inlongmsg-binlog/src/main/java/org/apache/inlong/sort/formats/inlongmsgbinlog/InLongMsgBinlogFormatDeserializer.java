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
import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;
import org.apache.inlong.sort.formats.inlongmsg.row.AbstractInLongMsgFormatDeserializer;
import org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The deserializer for the records in TDMsgDBSync format.
 */
public final class InLongMsgBinlogFormatDeserializer extends AbstractInLongMsgFormatDeserializer {

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

    public InLongMsgBinlogFormatDeserializer(
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
                InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors));
    }

    public InLongMsgBinlogFormatDeserializer(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nullable String metadataFieldName,
            @Nonnull FailureHandler failureHandler) {
        this(
                rowFormatInfo,
                timeFieldName,
                attributesFieldName,
                metadataFieldName,
                failureHandler,
                false);
    }

    public InLongMsgBinlogFormatDeserializer(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nullable String metadataFieldName,
            boolean ignoreErrors,
            boolean includeUpdateBefore) {
        this(
                rowFormatInfo,
                timeFieldName,
                attributesFieldName,
                metadataFieldName,
                InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors),
                includeUpdateBefore);
    }

    public InLongMsgBinlogFormatDeserializer(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nullable String metadataFieldName,
            @Nonnull FailureHandler failureHandler,
            boolean includeUpdateBefore) {
        super(failureHandler);

        this.rowFormatInfo = rowFormatInfo;
        this.timeFieldName = timeFieldName;
        this.attributesFieldName = attributesFieldName;
        this.metadataFieldName = metadataFieldName;
        this.includeUpdateBefore = includeUpdateBefore;
    }

    @Override
    protected InLongMsgHead parseHead(String attr) {
        return InLongMsgBinlogUtils.parseHead(attr);
    }

    @Override
    protected List<InLongMsgBody> parseBodyList(byte[] bytes) {
        return Collections.singletonList(InLongMsgBinlogUtils.parseBody(bytes));
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
    protected List<Row> convertRows(InLongMsgHead head, InLongMsgBody body) throws IOException {
        return InLongMsgBinlogUtils.getRows(
                rowFormatInfo,
                timeFieldName,
                attributesFieldName,
                metadataFieldName,
                head.getAttributes(),
                body.getData(),
                includeUpdateBefore);
    }

    /**
     * The builder for {@link InLongMsgBinlogFormatDeserializer}.
     */
    public static class Builder extends InLongMsgBinlogFormatBuilder<Builder> {

        public Builder(RowFormatInfo rowFormatInfo) {
            super(rowFormatInfo);
        }

        public InLongMsgBinlogFormatDeserializer build() {
            return new InLongMsgBinlogFormatDeserializer(
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

        InLongMsgBinlogFormatDeserializer that = (InLongMsgBinlogFormatDeserializer) object;
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
