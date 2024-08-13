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

import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.base.TextFormatBuilder;
import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;
import org.apache.inlong.sort.formats.inlongmsg.row.AbstractInLongMsgFormatDeserializer;
import org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.types.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsgtlogkv.InLongMsgTlogKvUtils.DEFAULT_INLONGMSG_TLOGKV_CHARSET;

/**
 * The deserializer for the records in InLongMsgTlogKV format.
 */
public final class InLongMsgTlogKvFormatDeserializer extends AbstractInLongMsgFormatDeserializer {

    private static final long serialVersionUID = 1L;

    /**
     * Format information describing the result type.
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
     * The charset of the text.
     */
    @Nonnull
    private final String charset;

    /**
     * The delimiter between fields.
     */
    @Nonnull
    private final Character delimiter;

    /**
     * The delimiter between entries.
     */
    @Nonnull
    private final Character entryDelimiter;

    /**
     * The delimiter between key and value.
     */
    @Nonnull
    private final Character kvDelimiter;

    /**
     * Escape character. Null if escaping is disabled.
     */
    @Nullable
    private final Character escapeChar;

    /**
     * Quote character. Null if quoting is disabled.
     */
    @Nullable
    private final Character quoteChar;

    /**
     * The literal represented null values, default "".
     */
    @Nullable
    private final String nullLiteral;

    public InLongMsgTlogKvFormatDeserializer(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            @Nonnull Boolean ignoreErrors) {
        this(
                rowFormatInfo,
                timeFieldName,
                attributesFieldName,
                charset,
                delimiter,
                entryDelimiter,
                kvDelimiter,
                escapeChar,
                quoteChar,
                nullLiteral,
                InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors));
    }

    public InLongMsgTlogKvFormatDeserializer(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            @Nonnull FailureHandler failureHandler) {
        super(failureHandler);

        this.rowFormatInfo = rowFormatInfo;
        this.timeFieldName = timeFieldName;
        this.attributesFieldName = attributesFieldName;
        this.charset = charset;
        this.delimiter = delimiter;
        this.entryDelimiter = entryDelimiter;
        this.kvDelimiter = kvDelimiter;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.nullLiteral = nullLiteral;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return InLongMsgUtils.decorateRowTypeWithNeededHeadFields(timeFieldName, attributesFieldName, rowFormatInfo);
    }

    @Override
    protected InLongMsgHead parseHead(String attr) throws Exception {
        return InLongMsgTlogKvUtils.parseHead(attr);
    }

    @Override
    protected List<InLongMsgBody> parseBodyList(byte[] bytes) throws Exception {
        return Collections.singletonList(
                InLongMsgTlogKvUtils.parseBody(
                        bytes,
                        charset,
                        delimiter,
                        entryDelimiter,
                        kvDelimiter,
                        escapeChar,
                        quoteChar));
    }

    @Override
    protected List<Row> convertRows(InLongMsgHead head, InLongMsgBody body) throws Exception {
        Row dataRow =
                InLongMsgTlogKvUtils.deserializeRow(
                        rowFormatInfo,
                        nullLiteral,
                        head.getPredefinedFields(),
                        body.getEntries());

        Row row = InLongMsgUtils.decorateRowWithNeededHeadFields(
                timeFieldName,
                attributesFieldName,
                head.getTime(),
                head.getAttributes(),
                dataRow);

        return Collections.singletonList(row);
    }

    /**
     * The builder for {@link InLongMsgTlogKvFormatDeserializer}.
     */
    public static class Builder extends TextFormatBuilder<Builder> {

        private String timeFieldName = DEFAULT_TIME_FIELD_NAME;
        private String attributesFieldName = DEFAULT_ATTRIBUTES_FIELD_NAME;
        private Character delimiter = DEFAULT_DELIMITER;
        private Character entryDelimiter = DEFAULT_ENTRY_DELIMITER;
        private Character kvDelimiter = DEFAULT_KV_DELIMITER;

        public Builder(RowFormatInfo rowFormatInfo) {
            super(rowFormatInfo);

            this.charset = DEFAULT_INLONGMSG_TLOGKV_CHARSET;
        }

        public Builder setTimeFieldName(String timeFieldName) {
            this.timeFieldName = timeFieldName;
            return this;
        }

        public Builder setAttributesFieldName(String attributesFieldName) {
            this.attributesFieldName = attributesFieldName;
            return this;
        }

        public Builder setDelimiter(Character delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public Builder setEntryDelimiter(Character entryDelimiter) {
            this.entryDelimiter = entryDelimiter;
            return this;
        }

        public Builder setKvDelimiter(Character kvDelimiter) {
            this.kvDelimiter = kvDelimiter;
            return this;
        }

        public Builder configure(DescriptorProperties descriptorProperties) {
            super.configure(descriptorProperties);

            descriptorProperties.getOptionalString(FORMAT_TIME_FIELD_NAME)
                    .ifPresent(this::setTimeFieldName);
            descriptorProperties.getOptionalString(FORMAT_ATTRIBUTES_FIELD_NAME)
                    .ifPresent(this::setAttributesFieldName);
            descriptorProperties.getOptionalCharacter(FORMAT_DELIMITER)
                    .ifPresent(this::setDelimiter);
            descriptorProperties.getOptionalCharacter(FORMAT_ENTRY_DELIMITER)
                    .ifPresent(this::setEntryDelimiter);
            descriptorProperties.getOptionalCharacter(FORMAT_KV_DELIMITER)
                    .ifPresent(this::setKvDelimiter);

            return this;
        }

        public InLongMsgTlogKvFormatDeserializer build() {
            return new InLongMsgTlogKvFormatDeserializer(
                    rowFormatInfo,
                    timeFieldName,
                    attributesFieldName,
                    charset,
                    delimiter,
                    entryDelimiter,
                    kvDelimiter,
                    escapeChar,
                    quoteChar,
                    nullLiteral,
                    ignoreErrors);
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

        InLongMsgTlogKvFormatDeserializer that = (InLongMsgTlogKvFormatDeserializer) object;
        return rowFormatInfo.equals(that.rowFormatInfo) &&
                Objects.equals(timeFieldName, that.timeFieldName) &&
                Objects.equals(attributesFieldName, that.attributesFieldName) &&
                charset.equals(that.charset) &&
                delimiter.equals(that.delimiter) &&
                entryDelimiter.equals(that.entryDelimiter) &&
                kvDelimiter.equals(that.kvDelimiter) &&
                Objects.equals(escapeChar, that.escapeChar) &&
                Objects.equals(quoteChar, that.quoteChar) &&
                Objects.equals(nullLiteral, that.nullLiteral);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rowFormatInfo, timeFieldName,
                attributesFieldName, charset, delimiter, entryDelimiter, kvDelimiter,
                escapeChar, quoteChar, nullLiteral);
    }
}
