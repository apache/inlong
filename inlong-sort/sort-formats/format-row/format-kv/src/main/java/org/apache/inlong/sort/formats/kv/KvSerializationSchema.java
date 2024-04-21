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

package org.apache.inlong.sort.formats.kv;

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
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.serializeBasicField;
import static org.apache.inlong.sort.formats.util.StringUtils.concatKv;

/**
 * The serializer for the records in kv format.
 */
public final class KvSerializationSchema extends DefaultSerializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(KvSerializationSchema.class);

    /**
     * Format information describing the result type.
     */
    private final RowFormatInfo rowFormatInfo;

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

    public KvSerializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull String charset,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            boolean ignoreErrors) {
        super(ignoreErrors);
        this.rowFormatInfo = rowFormatInfo;
        this.charset = charset;
        this.entryDelimiter = entryDelimiter;
        this.kvDelimiter = kvDelimiter;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.nullLiteral = nullLiteral;
    }

    public KvSerializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo) {
        this(
                rowFormatInfo,
                DEFAULT_CHARSET,
                DEFAULT_ENTRY_DELIMITER,
                DEFAULT_KV_DELIMITER,
                null,
                null,
                null,
                false);
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

            // the extra fields will be dropped
            for (int i = 0; i < fieldNames.length; ++i) {
                if (i >= row.getArity()) {
                    // the absent fields will be filled with null literal
                    fieldTexts[i] = nullLiteral == null ? "" : nullLiteral;
                } else {
                    Object field = row.getField(i);

                    String fieldText =
                            serializeBasicField(
                                    fieldNames[i],
                                    fieldFormatInfos[i],
                                    field,
                                    nullLiteral);

                    fieldTexts[i] = fieldText;
                }
            }

            String text =
                    concatKv(
                            fieldNames,
                            fieldTexts,
                            entryDelimiter,
                            kvDelimiter,
                            escapeChar,
                            quoteChar);

            return text.getBytes(Charset.forName(charset));
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Could not properly serialize kv. Row=[%s].", row), t);
        }
    }

    /**
     * Builder for {@link KvSerializationSchema}.
     */
    public static class Builder extends KvFormatBuilder<Builder> {

        public Builder(RowFormatInfo rowFormatInfo) {
            super(rowFormatInfo);
        }

        public KvSerializationSchema build() {
            return new KvSerializationSchema(
                    rowFormatInfo,
                    charset,
                    entryDelimiter,
                    kvDelimiter,
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

        KvSerializationSchema that = (KvSerializationSchema) o;
        return Objects.equals(rowFormatInfo, that.rowFormatInfo) &&
                Objects.equals(charset, that.charset) &&
                Objects.equals(entryDelimiter, that.entryDelimiter) &&
                Objects.equals(kvDelimiter, that.kvDelimiter) &&
                Objects.equals(escapeChar, that.escapeChar) &&
                Objects.equals(quoteChar, that.quoteChar) &&
                Objects.equals(nullLiteral, that.nullLiteral);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowFormatInfo, charset, entryDelimiter, kvDelimiter,
                escapeChar, quoteChar, nullLiteral);
    }
}
