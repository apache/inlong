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

package org.apache.inlong.sort.formats.csv;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.base.DefaultDeserializationSchema;
import org.apache.inlong.sort.formats.base.FieldToRowDataConverters;
import org.apache.inlong.sort.formats.base.FieldToRowDataConverters.FieldToRowDataConverter;
import org.apache.inlong.sort.formats.base.FormatMsg;
import org.apache.inlong.sort.formats.base.TableFormatUtils;
import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_QUOTE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.getFormatValueLength;
import static org.apache.inlong.sort.formats.util.StringUtils.splitCsv;

/**
 * Deserialization schema from CSV to Flink Table & SQL internal data structures.
 *
 * <p>Deserializes a <code>byte[]</code> message as a {@link JsonNode} and converts it to {@link
 * RowData}.
 *
 * <p>Failure during deserialization are forwarded as wrapped {@link IOException}s.
 */
@Internal
public final class CsvRowDataDeserializationSchema extends DefaultDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CsvRowDataDeserializationSchema.class);

    /**
     * Type information describing the result type.
     */
    @Nonnull
    private final TypeInformation<RowData> resultTypeInfo;

    /**
     * Format information describing the result type.
     */
    @Nonnull
    private final RowFormatInfo rowFormatInfo;

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

    private final FieldToRowDataConverter[] converters;

    public CsvRowDataDeserializationSchema(
            @Nonnull TypeInformation<RowData> resultTypeInfo,
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            Boolean ignoreErrors) {
        super(ignoreErrors);
        this.resultTypeInfo = resultTypeInfo;
        this.rowFormatInfo = rowFormatInfo;
        this.charset = charset;
        this.delimiter = delimiter;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.nullLiteral = nullLiteral;

        String[] fieldNames = rowFormatInfo.getFieldNames();
        this.fieldNameSize = (fieldNames == null ? 0 : fieldNames.length);

        converters = Arrays.stream(rowFormatInfo.getFieldFormatInfos())
                .map(formatInfo -> FieldToRowDataConverters.createConverter(
                        TableFormatUtils.deriveLogicalType(formatInfo)))
                .toArray(FieldToRowDataConverter[]::new);
    }

    public CsvRowDataDeserializationSchema(
            @Nonnull TypeInformation<RowData> resultTypeInfo,
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            FailureHandler failureHandler) {
        super(failureHandler);
        this.resultTypeInfo = resultTypeInfo;
        this.rowFormatInfo = rowFormatInfo;
        this.charset = charset;
        this.delimiter = delimiter;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.nullLiteral = nullLiteral;

        String[] fieldNames = rowFormatInfo.getFieldNames();
        this.fieldNameSize = (fieldNames == null ? 0 : fieldNames.length);

        converters = Arrays.stream(rowFormatInfo.getFieldFormatInfos())
                .map(formatInfo -> FieldToRowDataConverters.createConverter(
                        TableFormatUtils.deriveLogicalType(formatInfo)))
                .toArray(FieldToRowDataConverter[]::new);
    }

    /**
     * A builder for creating a {@link CsvRowDataDeserializationSchema}.
     */
    @Internal
    public static class Builder {

        private final RowFormatInfo rowFormatInfo;

        private final TypeInformation<RowData> resultTypeInfo;

        private String charset = DEFAULT_CHARSET;

        protected char delimiter = DEFAULT_DELIMITER;

        private Character escapeChar = DEFAULT_ESCAPE_CHARACTER;

        private Character quoteChar = DEFAULT_QUOTE_CHARACTER;

        private String nullLiteral = DEFAULT_NULL_LITERAL;

        private Boolean ignoreErrors = DEFAULT_IGNORE_ERRORS;

        /**
         * Creates a CSV deserialization schema for the given {@link TypeInformation} with optional
         * parameters.
         */
        public Builder(RowFormatInfo rowFormatInfo, TypeInformation<RowData> resultTypeInfo) {
            Preconditions.checkNotNull(rowFormatInfo, "RowFormatInfo must not be null.");
            Preconditions.checkNotNull(resultTypeInfo, "Result type information must not be null.");
            this.rowFormatInfo = rowFormatInfo;
            this.resultTypeInfo = resultTypeInfo;
        }

        public Builder setCharset(String charset) {
            this.charset = charset;
            return this;
        }

        public Builder setFieldDelimiter(char delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public Builder setQuoteCharacter(char c) {
            this.quoteChar = c;
            return this;
        }

        public Builder setEscapeCharacter(char c) {
            this.escapeChar = c;
            return this;
        }

        public Builder setNullLiteral(String nullLiteral) {
            Preconditions.checkNotNull(nullLiteral, "Null literal must not be null.");
            this.nullLiteral = nullLiteral;
            return this;
        }

        public Builder setIgnoreErrors(Boolean ignoreErrors) {
            this.ignoreErrors = ignoreErrors;
            return this;
        }

        public CsvRowDataDeserializationSchema build() {
            return new CsvRowDataDeserializationSchema(
                    resultTypeInfo,
                    rowFormatInfo,
                    charset,
                    delimiter,
                    escapeChar,
                    quoteChar,
                    nullLiteral,
                    ignoreErrors);
        }
    }

    @Override
    public RowData deserializeInternal(@Nullable byte[] message) throws Exception {
        if (message == null) {
            return null;
        }
        String text = new String(message, Charset.forName(charset));

        try {
            String[] fieldNames = rowFormatInfo.getFieldNames();
            FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

            String[] fieldTexts = splitCsv(text, delimiter, escapeChar, quoteChar);

            checkFieldNameSize(text, fieldTexts.length, fieldNameSize, failureHandler);

            GenericRowData rowData = new GenericRowData(fieldNames.length);

            for (int i = 0; i < fieldNames.length; ++i) {
                if (i >= fieldTexts.length) {
                    rowData.setField(i, null);
                } else {
                    Object field =
                            TableFormatUtils.deserializeBasicField(
                                    fieldNames[i],
                                    fieldFormatInfos[i],
                                    fieldTexts[i],
                                    nullLiteral, null, null, text, failureHandler);

                    rowData.setField(i, converters[i].convert(field));
                }
            }
            return rowData;
        } catch (Throwable t) {
            failureHandler.onParsingMsgFailure(text, new RuntimeException(
                    String.format("Could not properly deserialize csv. Text=[{}].", text), t));
        }
        return null;
    }

    @Override
    public FormatMsg deserializeFormatMsg(byte[] message) throws Exception {
        if (message == null) {
            return null;
        }
        String text = new String(message, Charset.forName(charset));
        long rowDataLength = 0L;
        try {
            String[] fieldNames = rowFormatInfo.getFieldNames();
            FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

            String[] fieldTexts = splitCsv(text, delimiter, escapeChar, quoteChar);

            checkFieldNameSize(text, fieldTexts.length, fieldNameSize, failureHandler);

            GenericRowData rowData = new GenericRowData(fieldNames.length);

            for (int i = 0; i < fieldNames.length; ++i) {
                if (i >= fieldTexts.length) {
                    rowData.setField(i, null);
                } else {
                    Object field =
                            TableFormatUtils.deserializeBasicField(
                                    fieldNames[i],
                                    fieldFormatInfos[i],
                                    fieldTexts[i],
                                    nullLiteral, null, null, text, failureHandler);

                    rowData.setField(i, converters[i].convert(field));
                    rowDataLength += getFormatValueLength(fieldFormatInfos[i], fieldTexts[i]);
                }
            }
            return new FormatMsg(rowData, rowDataLength);
        } catch (Throwable t) {
            failureHandler.onParsingMsgFailure(text, new RuntimeException(
                    String.format("Could not properly deserialize csv. Text=[{}].", text), t));
        }
        return null;
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
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
        CsvRowDataDeserializationSchema that = (CsvRowDataDeserializationSchema) o;
        return resultTypeInfo.equals(that.resultTypeInfo) &&
                rowFormatInfo.equals(that.rowFormatInfo) &&
                charset.equals(that.charset) &&
                delimiter.equals(that.delimiter) &&
                Objects.equals(escapeChar, that.escapeChar) &&
                Objects.equals(quoteChar, that.quoteChar) &&
                Objects.equals(nullLiteral, that.nullLiteral);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultTypeInfo, rowFormatInfo, charset, delimiter, escapeChar, quoteChar,
                nullLiteral);
    }
}
