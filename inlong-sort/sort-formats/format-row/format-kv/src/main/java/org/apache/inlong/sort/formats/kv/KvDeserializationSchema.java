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
import org.apache.inlong.sort.formats.base.DefaultDeserializationSchema;
import org.apache.inlong.sort.formats.base.TableFormatForRowUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Objects;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.deserializeBasicField;
import static org.apache.inlong.sort.formats.util.StringUtils.splitKv;

/**
 * Deserializer from KV to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> to {@link Row}.
 *
 * <p>Failure during deserialization are forwarded as wrapped {@link IOException}s.
 */
public final class KvDeserializationSchema extends DefaultDeserializationSchema<Row> {

    private static final long serialVersionUID = 1L;

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

    public KvDeserializationSchema(
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
        this.entryDelimiter = entryDelimiter;
        this.kvDelimiter = kvDelimiter;
        this.charset = charset;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.nullLiteral = nullLiteral;
    }

    public KvDeserializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo) {
        this(
                rowFormatInfo,
                DEFAULT_CHARSET,
                DEFAULT_ENTRY_DELIMITER,
                DEFAULT_KV_DELIMITER,
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
    public boolean isEndOfStream(Row row) {
        return false;
    }

    @Override
    protected Row deserializeInternal(byte[] bytes) throws IOException {

        String text = new String(bytes, Charset.forName(charset));

        try {
            Map<String, String> fieldTexts =
                    splitKv(text, entryDelimiter, kvDelimiter, escapeChar, quoteChar);

            String[] fieldNames = rowFormatInfo.getFieldNames();
            FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

            Row row = new Row(fieldFormatInfos.length);

            for (int i = 0; i < fieldFormatInfos.length; ++i) {

                String fieldName = fieldNames[i];
                FormatInfo fieldFormatInfo = fieldFormatInfos[i];

                String fieldText = fieldTexts.get(fieldName);

                Object field =
                        deserializeBasicField(
                                fieldName,
                                fieldFormatInfo,
                                fieldText,
                                nullLiteral, null);
                row.setField(i, field);
            }

            return row;
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Could not properly deserialize kv. Text=[%s].", text), t);
        }
    }

    /**
     * Builder for {@link KvDeserializationSchema}.
     */
    public static class Builder extends KvFormatBuilder<Builder> {

        public Builder(RowFormatInfo rowFormatInfo) {
            super(rowFormatInfo);
        }

        public KvDeserializationSchema build() {
            return new KvDeserializationSchema(
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

        KvDeserializationSchema that = (KvDeserializationSchema) o;
        return rowFormatInfo.equals(that.rowFormatInfo) &&
                Objects.equals(charset, that.charset) &&
                Objects.equals(entryDelimiter, that.entryDelimiter) &&
                Objects.equals(kvDelimiter, that.kvDelimiter) &&
                Objects.equals(escapeChar, that.escapeChar) &&
                Objects.equals(quoteChar, that.quoteChar) &&
                Objects.equals(nullLiteral, that.nullLiteral) &&
                ignoreErrors == that.ignoreErrors;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowFormatInfo, charset, entryDelimiter, kvDelimiter,
                escapeChar, quoteChar, nullLiteral, ignoreErrors);
    }
}