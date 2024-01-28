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

import org.apache.inlong.sort.formats.base.DefaultSerializationSchema;
import org.apache.inlong.sort.formats.base.RowDataToFieldConverters;
import org.apache.inlong.sort.formats.base.TableFormatForRowDataUtils;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.RowFormatInfo;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Objects;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_QUOTE_CHARACTER;
import static org.apache.inlong.sort.formats.util.StringUtils.concatCsv;

/**
 * Serialization schema that serializes an object of Flink Table & SQL internal data structure into
 * a CSV bytes.
 *
 * <p>Serializes the input row into a {@link JsonNode} and converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link
 * CsvRowDataDeserializationSchema}.
 */
@PublicEvolving
public final class CsvRowDataSerializationSchema extends DefaultSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CsvRowDataDeserializationSchema.class);

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

    private final RowDataToFieldConverters.RowFieldConverter[] rowFieldConverters;

    public CsvRowDataSerializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            Boolean ignoreErrors) {
        super(ignoreErrors);
        this.rowFormatInfo = rowFormatInfo;
        this.charset = charset;
        this.delimiter = delimiter;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.nullLiteral = nullLiteral;

        FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();
        rowFieldConverters = new RowDataToFieldConverters.RowFieldConverter[fieldFormatInfos.length];
        for (int i = 0; i < rowFieldConverters.length; i++) {
            rowFieldConverters[i] = RowDataToFieldConverters.createNullableRowFieldConverter(
                    TableFormatForRowDataUtils.deriveLogicalType(fieldFormatInfos[i]));
        }
    }

    /**
     * A builder for creating a {@link CsvRowDataSerializationSchema}.
     */
    @PublicEvolving
    public static class Builder {

        private final RowFormatInfo rowFormatInfo;

        private String charset = DEFAULT_CHARSET;

        protected char delimiter = DEFAULT_DELIMITER;

        private Character escapeChar = DEFAULT_ESCAPE_CHARACTER;

        private Character quoteChar = DEFAULT_QUOTE_CHARACTER;

        private String nullLiteral = DEFAULT_NULL_LITERAL;

        private Boolean ignoreErrors = DEFAULT_IGNORE_ERRORS;

        /**
         * Creates a {@link CsvRowDataSerializationSchema} expecting the given {@link RowType}.
         *
         * @param rowFormatInfo logical row format info used to create schema.
         */
        public Builder(RowFormatInfo rowFormatInfo) {
            Preconditions.checkNotNull(rowFormatInfo, "Row format info must not be null.");
            this.rowFormatInfo = rowFormatInfo;
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

        public Builder setIgnoreErrors(boolean ingoreErrors) {
            this.ignoreErrors = ingoreErrors;
            return this;
        }

        public CsvRowDataSerializationSchema build() {
            return new CsvRowDataSerializationSchema(
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
    public byte[] serializeInternal(RowData rowData) {
        if (rowData == null) {
            return null;
        }
        try {
            String[] fieldNames = rowFormatInfo.getFieldNames();
            FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

            if (rowData.getArity() != fieldFormatInfos.length) {
                LOG.warn("The number of fields mismatches: expected=[{}], actual=[{}]. Row=[{}].",
                        fieldNames.length, rowData.getArity(), rowData);
            }

            String[] fieldTexts = new String[fieldNames.length];

            // The extra fields will be dropped.
            for (int i = 0; i < fieldNames.length; ++i) {
                if (i >= rowData.getArity()) {
                    // The absent fields will be filled with null literal
                    fieldTexts[i] = nullLiteral == null ? "" : nullLiteral;
                } else {
                    String fieldText =
                            TableFormatForRowDataUtils.serializeBasicField(
                                    fieldNames[i],
                                    fieldFormatInfos[i],
                                    rowFieldConverters[i].convert(rowData, i),
                                    nullLiteral);
                    fieldTexts[i] = fieldText;
                }
            }

            String result =
                    concatCsv(fieldTexts, delimiter, escapeChar, quoteChar);

            return result.getBytes(Charset.forName(charset));
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Could not properly serialize csv. Row=[%s].", rowData), t);
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
        CsvRowDataSerializationSchema that = (CsvRowDataSerializationSchema) o;
        return rowFormatInfo.equals(that.rowFormatInfo) &&
                charset.equals(that.charset) &&
                delimiter.equals(that.delimiter) &&
                Objects.equals(escapeChar, that.escapeChar) &&
                Objects.equals(quoteChar, that.quoteChar) &&
                Objects.equals(nullLiteral, that.nullLiteral);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rowFormatInfo, charset, delimiter, escapeChar, quoteChar, nullLiteral);
    }
}
