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
import org.apache.inlong.sort.formats.base.DefaultSerializationSchema;

import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Objects;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.serializeBasicField;
import static org.apache.inlong.sort.formats.util.StringUtils.concatCsv;

/**
 * The serializer for the records in csv format.
 */
public class CsvSerializationSchema extends DefaultSerializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CsvSerializationSchema.class);

    /**
     * Format information describing the result type.
     */
    private final RowFormatInfo rowFormatInfo;

    /**
     * The charset for the text.
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

    public CsvSerializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            boolean ignoreErrors) {
        super(ignoreErrors);
        this.rowFormatInfo = rowFormatInfo;
        this.charset = charset;
        this.delimiter = delimiter;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.nullLiteral = nullLiteral;
    }

    public CsvSerializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo) {
        this(
                rowFormatInfo,
                DEFAULT_CHARSET,
                DEFAULT_DELIMITER,
                null,
                null,
                null,
                DEFAULT_IGNORE_ERRORS);
    }

    @Override
    protected byte[] serializeInternal(Row row) {
        if (row == null) {
            return null;
        }

        try {
            String[] fieldNames = rowFormatInfo.getFieldNames();
            FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

            if (row.getArity() != fieldFormatInfos.length) {
                LOG.warn("The number of fields mismatches: expected=[{}], actual=[{}]. Row=[{}].",
                        fieldNames.length, row.getArity(), row);
            }

            String[] fieldTexts = new String[fieldNames.length];

            // The extra fields will be dropped.
            for (int i = 0; i < fieldNames.length; ++i) {
                if (i >= row.getArity()) {
                    // The absent fields will be filled with null literal
                    fieldTexts[i] = nullLiteral == null ? "" : nullLiteral;
                } else {
                    String fieldText =
                            serializeBasicField(
                                    fieldNames[i],
                                    fieldFormatInfos[i],
                                    row.getField(i),
                                    nullLiteral);
                    fieldTexts[i] = fieldText;
                }
            }

            String result =
                    concatCsv(fieldTexts, delimiter, escapeChar, quoteChar);

            return result.getBytes(Charset.forName(charset));
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Could not properly serialize csv. Row=[%s].", row), t);
        }
    }

    /**
     * Builder for {@link CsvSerializationSchema}.
     */
    public static class Builder extends CsvFormatBuilder<Builder> {

        public Builder(RowFormatInfo rowFormatInfo) {
            super(rowFormatInfo);
        }

        public CsvSerializationSchema build() {
            return new CsvSerializationSchema(
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CsvSerializationSchema that = (CsvSerializationSchema) o;
        return Objects.equals(rowFormatInfo, that.rowFormatInfo) &&
                Objects.equals(charset, that.charset) &&
                Objects.equals(delimiter, that.delimiter) &&
                Objects.equals(escapeChar, that.escapeChar) &&
                Objects.equals(quoteChar, that.quoteChar) &&
                Objects.equals(nullLiteral, that.nullLiteral);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowFormatInfo, charset, delimiter,
                escapeChar, quoteChar, nullLiteral);
    }
}