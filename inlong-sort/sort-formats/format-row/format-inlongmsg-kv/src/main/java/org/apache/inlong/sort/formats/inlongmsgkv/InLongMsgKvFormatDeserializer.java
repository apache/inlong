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

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_LINE_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_LINE_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_PREDEFINED_FIELD;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_RETAIN_PREDEFINED_FIELD;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsgkv.InLongMsgKvUtils.DEFAULT_INLONGMSGKV_CHARSET;

/**
 * The deserializer for the records in InLongMsgKv format.
 */
public final class InLongMsgKvFormatDeserializer extends AbstractInLongMsgFormatDeserializer {

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
     * The delimiter between lines.
     */
    @Nullable
    private final Character lineDelimiter;

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

    /**
     * True if the predefinedField existed, default true.
     */
    private boolean retainPredefinedField = true;

    public InLongMsgKvFormatDeserializer(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nonnull String charset,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character lineDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            @Nonnull Boolean ignoreErrors,
            boolean retainPredefinedField) {
        this(
                rowFormatInfo,
                timeFieldName,
                attributesFieldName,
                charset,
                entryDelimiter,
                kvDelimiter,
                lineDelimiter,
                escapeChar,
                quoteChar,
                nullLiteral,
                InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors));
        this.retainPredefinedField = retainPredefinedField;
    }

    public InLongMsgKvFormatDeserializer(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nonnull String charset,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character lineDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            @Nonnull Boolean ignoreErrors) {
        this(
                rowFormatInfo,
                timeFieldName,
                attributesFieldName,
                charset,
                entryDelimiter,
                kvDelimiter,
                lineDelimiter,
                escapeChar,
                quoteChar,
                nullLiteral,
                InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors));
    }

    public InLongMsgKvFormatDeserializer(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nonnull String charset,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character lineDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            @Nonnull FailureHandler failureHandler) {
        super(failureHandler);

        this.rowFormatInfo = rowFormatInfo;
        this.timeFieldName = timeFieldName;
        this.attributesFieldName = attributesFieldName;
        this.charset = charset;
        this.entryDelimiter = entryDelimiter;
        this.kvDelimiter = kvDelimiter;
        this.lineDelimiter = lineDelimiter;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.nullLiteral = nullLiteral;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return InLongMsgUtils.decorateRowTypeWithNeededHeadFields(timeFieldName, attributesFieldName, rowFormatInfo);
    }

    @Override
    protected InLongMsgHead parseHead(String attr) {
        return InLongMsgKvUtils.parseHead(attr);
    }

    @Override
    protected List<InLongMsgBody> parseBodyList(byte[] bytes) {
        return InLongMsgKvUtils.parseBodyList(
                bytes,
                charset,
                entryDelimiter,
                kvDelimiter,
                lineDelimiter,
                escapeChar,
                quoteChar);
    }

    @Override
    protected List<Row> convertRows(InLongMsgHead head, InLongMsgBody body) throws Exception {
        Row dataRow =
                InLongMsgKvUtils.deserializeRow(
                        rowFormatInfo,
                        nullLiteral,
                        retainPredefinedField ? head.getPredefinedFields() : Collections.emptyList(),
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
     * The builder for {@link InLongMsgKvFormatDeserializer}.
     */
    public static class Builder extends TextFormatBuilder<Builder> {

        private String timeFieldName = DEFAULT_TIME_FIELD_NAME;
        private String attributesFieldName = DEFAULT_ATTRIBUTES_FIELD_NAME;
        private Character entryDelimiter = DEFAULT_ENTRY_DELIMITER;
        private Character kvDelimiter = DEFAULT_KV_DELIMITER;
        private Character lineDelimiter = DEFAULT_LINE_DELIMITER;
        private Boolean retainPredefinedField = DEFAULT_PREDEFINED_FIELD;

        public Builder(RowFormatInfo rowFormatInfo) {
            super(rowFormatInfo);

            this.charset = DEFAULT_INLONGMSGKV_CHARSET;
        }

        public Builder setTimeFieldName(String timeFieldName) {
            this.timeFieldName = timeFieldName;
            return this;
        }

        public Builder setAttributesFieldName(String attributesFieldName) {
            this.attributesFieldName = attributesFieldName;
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

        public Builder setLineDelimiter(Character lineDelimiter) {
            this.lineDelimiter = lineDelimiter;
            return this;
        }

        public Builder setRetainPredefinedField(Boolean retainPredefinedField) {
            this.retainPredefinedField = retainPredefinedField;
            return this;
        }

        public Builder configure(DescriptorProperties descriptorProperties) {
            super.configure(descriptorProperties);

            descriptorProperties.getOptionalString(FORMAT_TIME_FIELD_NAME)
                    .ifPresent(this::setTimeFieldName);
            descriptorProperties.getOptionalString(FORMAT_ATTRIBUTES_FIELD_NAME)
                    .ifPresent(this::setAttributesFieldName);
            descriptorProperties.getOptionalCharacter(FORMAT_ENTRY_DELIMITER)
                    .ifPresent(this::setEntryDelimiter);
            descriptorProperties.getOptionalCharacter(FORMAT_KV_DELIMITER)
                    .ifPresent(this::setKvDelimiter);
            descriptorProperties.getOptionalCharacter(FORMAT_LINE_DELIMITER)
                    .ifPresent(this::setLineDelimiter);
            descriptorProperties.getOptionalBoolean(FORMAT_RETAIN_PREDEFINED_FIELD)
                    .ifPresent(this::setRetainPredefinedField);

            return this;
        }

        public InLongMsgKvFormatDeserializer build() {
            return new InLongMsgKvFormatDeserializer(
                    rowFormatInfo,
                    timeFieldName,
                    attributesFieldName,
                    charset,
                    entryDelimiter,
                    kvDelimiter,
                    lineDelimiter,
                    escapeChar,
                    quoteChar,
                    nullLiteral,
                    ignoreErrors,
                    retainPredefinedField);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        InLongMsgKvFormatDeserializer that = (InLongMsgKvFormatDeserializer) o;
        return rowFormatInfo.equals(that.rowFormatInfo) &&
                Objects.equals(timeFieldName, that.timeFieldName) &&
                Objects.equals(attributesFieldName, that.attributesFieldName) &&
                charset.equals(that.charset) &&
                entryDelimiter.equals(that.entryDelimiter) &&
                kvDelimiter.equals(that.kvDelimiter) &&
                Objects.equals(lineDelimiter, that.lineDelimiter) &&
                Objects.equals(escapeChar, that.escapeChar) &&
                Objects.equals(quoteChar, that.quoteChar) &&
                Objects.equals(nullLiteral, that.nullLiteral) &&
                Objects.equals(retainPredefinedField, that.retainPredefinedField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rowFormatInfo, timeFieldName,
                attributesFieldName, charset, entryDelimiter, kvDelimiter, lineDelimiter,
                escapeChar, quoteChar, nullLiteral, retainPredefinedField);
    }
}
