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
import org.apache.inlong.sort.formats.base.RowDataToFieldConverters;
import org.apache.inlong.sort.formats.base.TableFormatForRowDataUtils;

import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Objects;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_QUOTE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.serializeBasicField;
import static org.apache.inlong.sort.formats.util.StringUtils.concatKv;

/**
 * Serialization schema that serializes an object of Flink Table & SQL internal data structure into
 * a KV format bytes.
 *
 * <p> Serialize the input row into a string in a form of "k1=v1&k2=v2" and converts it into
 * bytes.</p>
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using
 * {@link KvRowDataDeserializationSchema}.</p>
 */
public class KvRowDataSerializationSchema extends DefaultSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(KvRowDataSerializationSchema.class);

    @Nonnull
    private final RowFormatInfo rowFormatInfo;

    @Nonnull
    private final String charset;

    @Nonnull
    private final Character entryDelimiter;

    @Nonnull
    private final Character kvDelimiter;

    @Nullable
    private final Character escapeChar;

    @Nullable
    private final Character quoteChar;

    @Nullable
    private final String nullLiteral;

    private final RowDataToFieldConverters.RowFieldConverter[] rowFieldConverters;

    public KvRowDataSerializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo) {
        this(
                rowFormatInfo,
                DEFAULT_CHARSET,
                DEFAULT_ENTRY_DELIMITER,
                DEFAULT_KV_DELIMITER,
                DEFAULT_ESCAPE_CHARACTER,
                DEFAULT_QUOTE_CHARACTER,
                DEFAULT_NULL_LITERAL);
    }

    public KvRowDataSerializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull String charset,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral) {
        this(rowFormatInfo, charset, entryDelimiter, kvDelimiter, escapeChar, quoteChar, nullLiteral,
                DEFAULT_IGNORE_ERRORS);
    }

    public KvRowDataSerializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull String charset,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            @Nullable boolean ignoreErrors) {
        super(ignoreErrors);
        this.rowFormatInfo = rowFormatInfo;
        FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

        rowFieldConverters = new RowDataToFieldConverters.RowFieldConverter[fieldFormatInfos.length];
        for (int i = 0; i < rowFieldConverters.length; i++) {
            rowFieldConverters[i] = RowDataToFieldConverters.createNullableRowFieldConverter(
                    TableFormatForRowDataUtils.deriveLogicalType(fieldFormatInfos[i]));
        }

        this.charset = charset;
        this.entryDelimiter = entryDelimiter;
        this.kvDelimiter = kvDelimiter;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.nullLiteral = nullLiteral;
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

            // the extra fields will be dropped
            for (int i = 0; i < fieldNames.length; i++) {
                if (i >= rowData.getArity()) {
                    fieldTexts[i] = nullLiteral == null ? "" : nullLiteral;
                } else {
                    Object field = rowFieldConverters[i].convert(rowData, i);
                    String fieldText = serializeBasicField(
                            fieldNames[i],
                            fieldFormatInfos[i],
                            field,
                            nullLiteral);

                    fieldTexts[i] = fieldText;
                }
            }

            String text = concatKv(
                    fieldNames,
                    fieldTexts,
                    entryDelimiter,
                    kvDelimiter,
                    escapeChar,
                    quoteChar);

            return text.getBytes(Charset.forName(charset));
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Could not properly serialize kv. Row=[%s]. FormatInfo=[%s].",
                            rowData, rowFormatInfo),
                    t);
        }
    }

    /**
     * Builder for {@link KvRowDataSerializationSchema}.
     */
    public static class Builder extends KvFormatBuilder<Builder> {

        public Builder(RowFormatInfo rowFormatInfo) {
            super(rowFormatInfo);
        }

        public KvRowDataSerializationSchema build() {
            return new KvRowDataSerializationSchema(
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

        if (!super.equals(o)) {
            return false;
        }

        KvRowDataSerializationSchema that = (KvRowDataSerializationSchema) o;
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
        return Objects.hash(super.hashCode(), rowFormatInfo, charset, entryDelimiter, kvDelimiter,
                escapeChar, quoteChar, nullLiteral);
    }
}
