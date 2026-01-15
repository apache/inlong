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

import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.base.FieldToRowDataConverters;
import org.apache.inlong.sort.formats.base.FieldToRowDataConverters.FieldToRowDataConverter;
import org.apache.inlong.sort.formats.base.FormatMsg;
import org.apache.inlong.sort.formats.base.TableFormatUtils;
import org.apache.inlong.sort.formats.base.TextFormatBuilder;
import org.apache.inlong.sort.formats.inlongmsg.AbstractInLongMsgFormatDeserializer;
import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_LINE_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_IS_DELETE_ESCAPE_CHAR;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_IS_DELETE_HEAD_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;

/**
 * The deserializer for the records in InLongMsgTlogCsv format.
 */
public final class InLongMsgTlogCsvFormatDeserializer extends AbstractInLongMsgFormatDeserializer {

    private static final Logger LOG = LoggerFactory.getLogger(InLongMsgTlogCsvFormatDeserializer.class);

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
    private final String charset;

    /**
     * The delimiter between fields.
     */
    @Nonnull
    private final Character delimiter;

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

    @Nullable
    private final Character lineDelimiter;

    @Nonnull
    private Boolean isIncludeFirstSegment = false;
    /**
     * The literal represented null values, default "".
     */
    @Nullable
    private final String nullLiteral;

    private final List<String> metadataKeys;

    private final FieldToRowDataConverter[] converters;

    /**
     * True if delete escape char while parsing.
     */
    private boolean isDeleteEscapeChar = DEFAULT_IS_DELETE_ESCAPE_CHAR;

    private boolean isDeleteHeadDelimiter = DEFAULT_IS_DELETE_HEAD_DELIMITER;

    @Deprecated
    public InLongMsgTlogCsvFormatDeserializer(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            List<String> metadataKeys,
            @Nonnull Boolean ignoreErrors) {
        this(
                rowFormatInfo,
                timeFieldName,
                attributesFieldName,
                charset,
                delimiter,
                escapeChar,
                quoteChar,
                DEFAULT_LINE_DELIMITER,
                nullLiteral,
                metadataKeys,
                DEFAULT_IS_DELETE_ESCAPE_CHAR,
                false,
                DEFAULT_IS_DELETE_HEAD_DELIMITER,
                InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors));
    }

    @Deprecated
    public InLongMsgTlogCsvFormatDeserializer(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable Character lineDelimiter,
            @Nullable String nullLiteral,
            List<String> metadataKeys,
            @Nonnull Boolean ignoreErrors) {
        this(
                rowFormatInfo,
                timeFieldName,
                attributesFieldName,
                charset,
                delimiter,
                escapeChar,
                quoteChar,
                lineDelimiter,
                nullLiteral,
                metadataKeys,
                DEFAULT_IS_DELETE_ESCAPE_CHAR,
                false,
                DEFAULT_IS_DELETE_HEAD_DELIMITER,
                InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors));
    }

    public InLongMsgTlogCsvFormatDeserializer(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            List<String> metadataKeys,
            @Nonnull Boolean isIncludeFirstSegment,
            @Nonnull FailureHandler failureHandler) {
        this(rowFormatInfo, timeFieldName, attributesFieldName,
                charset, delimiter, escapeChar, quoteChar,
                DEFAULT_LINE_DELIMITER,
                nullLiteral, metadataKeys, DEFAULT_IS_DELETE_ESCAPE_CHAR, isIncludeFirstSegment,
                DEFAULT_IS_DELETE_HEAD_DELIMITER, failureHandler);

    }

    public InLongMsgTlogCsvFormatDeserializer(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable Character lineDelimiter,
            @Nullable String nullLiteral,
            List<String> metadataKeys,
            @Nonnull Boolean isDeleteEscapeChar,
            @Nonnull Boolean isIncludeFirstSegment,
            @Nonnull Boolean isDeleteHeadDelimiter,
            @Nonnull FailureHandler failureHandler) {
        super(failureHandler);

        this.rowFormatInfo = rowFormatInfo;
        this.timeFieldName = timeFieldName;
        this.attributesFieldName = attributesFieldName;
        this.charset = charset;
        this.delimiter = delimiter;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.lineDelimiter = lineDelimiter;
        this.nullLiteral = nullLiteral;
        this.metadataKeys = metadataKeys;
        this.isIncludeFirstSegment = isIncludeFirstSegment;
        this.isDeleteHeadDelimiter = isDeleteHeadDelimiter;
        this.isDeleteEscapeChar = isDeleteEscapeChar;

        String[] fieldNames = rowFormatInfo.getFieldNames();
        this.fieldNameSize = (fieldNames == null ? 0 : fieldNames.length);

        converters = Arrays.stream(rowFormatInfo.getFieldFormatInfos())
                .map(formatInfo -> FieldToRowDataConverters.createConverter(
                        TableFormatUtils.deriveLogicalType(formatInfo)))
                .toArray(FieldToRowDataConverter[]::new);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InLongMsgUtils.decorateRowTypeWithNeededHeadFieldsAndMetadata(
                timeFieldName,
                attributesFieldName,
                rowFormatInfo,
                metadataKeys);
    }

    @Override
    protected InLongMsgHead parseHead(String attr) throws Exception {
        return InLongMsgTlogCsvUtils.parseHead(attr);
    }

    @Override
    protected List<InLongMsgBody> parseBodyList(byte[] bytes) throws Exception {
        return InLongMsgTlogCsvUtils.parseBody(bytes, charset, delimiter, escapeChar,
                quoteChar, lineDelimiter, isDeleteEscapeChar, isIncludeFirstSegment, isDeleteHeadDelimiter);
    }

    @Override
    protected List<RowData> convertRowDataList(InLongMsgHead head, InLongMsgBody body) throws Exception {
        List<String> predefinedFields = head.getPredefinedFields();
        List<String> fields = body.getFields();
        int actualNumFields = (predefinedFields == null ? 0 : predefinedFields.size())
                + (fields == null ? 0 : fields.size());
        checkFieldNameSize(head, body, actualNumFields, fieldNameSize, failureHandler);
        GenericRowData dataRow =
                InLongMsgTlogCsvUtils.deserializeRowData(
                        rowFormatInfo,
                        nullLiteral,
                        head.getPredefinedFields(),
                        body.getFields(),
                        converters, failureHandler);

        GenericRowData genericRowData = InLongMsgUtils.decorateRowDataWithNeededHeadFields(
                timeFieldName,
                attributesFieldName,
                head.getTime(),
                head.getAttributes(),
                dataRow);

        return Collections.singletonList(InLongMsgUtils.decorateRowWithMetaData(genericRowData, head, metadataKeys));
    }

    protected List<FormatMsg> convertFormatMsgList(InLongMsgHead head, InLongMsgBody body) throws Exception {
        List<String> predefinedFields = head.getPredefinedFields();
        List<String> fields = body.getFields();
        int actualNumFields = (predefinedFields == null ? 0 : predefinedFields.size())
                + (fields == null ? 0 : fields.size());
        checkFieldNameSize(head, body, actualNumFields, fieldNameSize, failureHandler);
        FormatMsg formatMsg =
                InLongMsgTlogCsvUtils.deserializeFormatMsgData(
                        rowFormatInfo,
                        nullLiteral,
                        head,
                        body,
                        converters, failureHandler);

        GenericRowData genericRowData = InLongMsgUtils.decorateRowDataWithNeededHeadFields(
                timeFieldName,
                attributesFieldName,
                head.getTime(),
                head.getAttributes(),
                (GenericRowData) formatMsg.getRowData());
        formatMsg.setRowData(InLongMsgUtils.decorateRowWithMetaData(genericRowData, head, metadataKeys));
        return Collections.singletonList(formatMsg);
    }

    /**
     * The builder for {@link InLongMsgTlogCsvFormatDeserializer}.
     */
    public static class Builder extends TextFormatBuilder<Builder> {

        private String timeFieldName = DEFAULT_TIME_FIELD_NAME;
        private String attributesFieldName = DEFAULT_ATTRIBUTES_FIELD_NAME;
        private Character delimiter = DEFAULT_DELIMITER;
        private Character lineDelimiter = DEFAULT_LINE_DELIMITER;
        private List<String> metadataKeys = Collections.emptyList();
        private boolean isIncludeFirstSegment = false;
        private boolean isDeleteEscapeChar = DEFAULT_IS_DELETE_ESCAPE_CHAR;
        private boolean isDeleteHeadDelimiter = DEFAULT_IS_DELETE_HEAD_DELIMITER;

        public Builder(RowFormatInfo rowFormatInfo) {
            super(rowFormatInfo);
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

        public Builder setLineDelimiter(Character lineDelimiter) {
            this.lineDelimiter = lineDelimiter;
            return this;
        }

        public Builder setMetadataKeys(List<String> metadataKeys) {
            this.metadataKeys = metadataKeys;
            return this;
        }

        public Builder setIncludeFirstSegment(boolean isIncludeFirstSegment) {
            this.isIncludeFirstSegment = isIncludeFirstSegment;
            return this;
        }

        public Builder setDeleteEscapeChar(Boolean isDeleteEscapeChar) {
            this.isDeleteEscapeChar = isDeleteEscapeChar;
            return this;
        }

        public Builder setDeleteHeadDelimiter(Boolean isDeleteHeadDelimiter) {
            this.isDeleteHeadDelimiter = isDeleteHeadDelimiter;
            return this;
        }

        public InLongMsgTlogCsvFormatDeserializer build() {
            return new InLongMsgTlogCsvFormatDeserializer(
                    rowFormatInfo,
                    timeFieldName,
                    attributesFieldName,
                    charset,
                    delimiter,
                    escapeChar,
                    quoteChar,
                    lineDelimiter,
                    nullLiteral,
                    metadataKeys,
                    isDeleteEscapeChar,
                    isIncludeFirstSegment,
                    isDeleteHeadDelimiter,
                    InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors));
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

        InLongMsgTlogCsvFormatDeserializer that = (InLongMsgTlogCsvFormatDeserializer) o;
        return rowFormatInfo.equals(that.rowFormatInfo) &&
                Objects.equals(timeFieldName, that.timeFieldName) &&
                Objects.equals(attributesFieldName, that.attributesFieldName) &&
                Objects.equals(charset, that.charset) &&
                delimiter.equals(that.delimiter) &&
                Objects.equals(escapeChar, that.escapeChar) &&
                Objects.equals(quoteChar, that.quoteChar) &&
                Objects.equals(lineDelimiter, that.lineDelimiter) &&
                Objects.equals(nullLiteral, that.nullLiteral) &&
                Objects.equals(metadataKeys, that.metadataKeys) &&
                Objects.equals(isIncludeFirstSegment, that.isIncludeFirstSegment) &&
                Objects.equals(isDeleteHeadDelimiter, that.isDeleteHeadDelimiter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rowFormatInfo, timeFieldName,
                attributesFieldName, charset, delimiter, escapeChar, quoteChar, lineDelimiter,
                nullLiteral, metadataKeys, isIncludeFirstSegment, isDeleteHeadDelimiter);
    }
}
