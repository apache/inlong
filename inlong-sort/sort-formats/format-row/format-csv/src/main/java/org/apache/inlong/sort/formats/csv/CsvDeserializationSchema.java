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
import org.apache.inlong.sort.formats.base.TableFormatForRowUtils;
import org.apache.inlong.sort.formats.base.util.LogCounter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import static org.apache.inlong.sort.formats.base.TableFormatUtils.deserializeBasicField;
import static org.apache.inlong.sort.formats.util.StringUtils.splitCsv;

/**
 * The deserializer for the records in csv format.
 */
public final class CsvDeserializationSchema extends DefaultDeserializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CsvDeserializationSchema.class);

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

    private long failureCount = 0;

    private LogCounter logCounter = new LogCounter(10, 1000, 60 * 1000);

    public CsvDeserializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            boolean ignoreErrors

    ) {
        super(ignoreErrors);
        this.rowFormatInfo = rowFormatInfo;
        this.charset = charset;
        this.delimiter = delimiter;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.nullLiteral = nullLiteral;
    }

    public CsvDeserializationSchema(
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

    @SuppressWarnings("unchecked")
    @Override
    public TypeInformation<Row> getProducedType() {
        return (TypeInformation<Row>) TableFormatForRowUtils.getType(rowFormatInfo.getTypeInfo());
    }

    @Override
    public boolean isEndOfStream(Row t) {
        return false;
    }

    @Override
    protected Row deserializeInternal(byte[] bytes) {
        String text = new String(bytes, Charset.forName(charset));

        try {
            String[] fieldNames = rowFormatInfo.getFieldNames();
            FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

            String[] fieldTexts = splitCsv(text, delimiter, escapeChar, quoteChar);
            if (fieldTexts.length != fieldNames.length) {
                failureCount = logCounter.increment();
                if (logCounter.shouldPrint()) {
                    LOG.warn("The number of fields mismatches: expected=[{}], actual=[{}]. " +
                            "The total mismatched data has accumulated to [{}].",
                            fieldNames.length, fieldTexts.length, failureCount);
                }
            }

            Row row = new Row(fieldNames.length);

            for (int i = 0; i < fieldNames.length; ++i) {
                if (i >= fieldTexts.length) {
                    row.setField(i, null);
                } else {
                    Object field =
                            deserializeBasicField(
                                    fieldNames[i],
                                    fieldFormatInfos[i],
                                    fieldTexts[i],
                                    nullLiteral, null);

                    row.setField(i, field);
                }
            }

            return row;
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Could not properly deserialize csv. Text=[%s].", text), t);
        }
    }

    /**
     * Builder for {@link CsvDeserializationSchema}.
     */
    public static class Builder extends CsvFormatBuilder<Builder> {

        public Builder(RowFormatInfo rowFormatInfo) {
            super(rowFormatInfo);
        }

        public CsvDeserializationSchema build() {
            return new CsvDeserializationSchema(
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

        CsvDeserializationSchema that = (CsvDeserializationSchema) o;
        return rowFormatInfo.equals(that.rowFormatInfo) &&
                Objects.equals(charset, that.charset) &&
                Objects.equals(delimiter, that.delimiter) &&
                Objects.equals(escapeChar, that.escapeChar) &&
                Objects.equals(quoteChar, that.quoteChar) &&
                Objects.equals(nullLiteral, that.nullLiteral) &&
                ignoreErrors == that.ignoreErrors;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowFormatInfo, charset, delimiter,
                escapeChar, quoteChar, nullLiteral, ignoreErrors);
    }
}
