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
import org.apache.inlong.sort.formats.base.FieldToRowDataConverters;
import org.apache.inlong.sort.formats.base.FormatMsg;
import org.apache.inlong.sort.formats.base.TableFormatForRowDataUtils;
import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_QUOTE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.deserializeBasicField;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.getFormatValueLength;
import static org.apache.inlong.sort.formats.util.StringUtils.splitKv;

/**
 * Deserialization schema from KV string bytes to Flink Table & SQL internal data structures.
 *
 * <p>Deserializes a <code>byte[]</code> messages as a Map and converts it to a {@link RowData}.</p>
 *
 * <p>Failure during deserialization are forwarded as wrapped {@link IOException}.</p>
 */
public class KvRowDataDeserializationSchema extends DefaultDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    @Nonnull
    private final RowFormatInfo rowFormatInfo;

    @Nonnull
    private final TypeInformation<RowData> producedTypeInfo;

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

    private final FieldToRowDataConverters.FieldToRowDataConverter[] converters;

    public KvRowDataDeserializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull TypeInformation<RowData> producedTypeInfo) {
        this(
                rowFormatInfo,
                producedTypeInfo,
                DEFAULT_CHARSET,
                DEFAULT_ENTRY_DELIMITER,
                DEFAULT_KV_DELIMITER,
                DEFAULT_ESCAPE_CHARACTER,
                DEFAULT_QUOTE_CHARACTER,
                DEFAULT_NULL_LITERAL);
    }

    public KvRowDataDeserializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull TypeInformation<RowData> producedTypeInfo,
            @Nonnull String charset,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral) {
        this(
                rowFormatInfo,
                producedTypeInfo,
                charset,
                entryDelimiter,
                kvDelimiter,
                escapeChar,
                quoteChar,
                nullLiteral,
                DEFAULT_IGNORE_ERRORS);
    }

    public KvRowDataDeserializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull TypeInformation<RowData> producedTypeInfo,
            @Nonnull String charset,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            @Nullable boolean ignoreErrors) {
        super(ignoreErrors);
        this.rowFormatInfo = rowFormatInfo;
        this.producedTypeInfo = producedTypeInfo;
        this.charset = charset;
        this.entryDelimiter = entryDelimiter;
        this.kvDelimiter = kvDelimiter;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.nullLiteral = nullLiteral;
        String[] fieldNames = rowFormatInfo.getFieldNames();
        this.fieldNameSize = (fieldNames == null ? 0 : fieldNames.length);

        converters = Arrays.stream(rowFormatInfo.getFieldFormatInfos())
                .map(formatInfo -> FieldToRowDataConverters.createConverter(
                        TableFormatForRowDataUtils.deriveLogicalType(formatInfo)))
                .toArray(FieldToRowDataConverters.FieldToRowDataConverter[]::new);
    }

    public KvRowDataDeserializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull TypeInformation<RowData> producedTypeInfo,
            @Nonnull String charset,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nullable String nullLiteral,
            @Nullable FailureHandler failureHandler) {
        super(failureHandler);
        this.rowFormatInfo = rowFormatInfo;
        this.producedTypeInfo = producedTypeInfo;
        this.charset = charset;
        this.entryDelimiter = entryDelimiter;
        this.kvDelimiter = kvDelimiter;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.nullLiteral = nullLiteral;

        String[] fieldNames = rowFormatInfo.getFieldNames();
        this.fieldNameSize = (fieldNames == null ? 0 : fieldNames.length);

        converters = Arrays.stream(rowFormatInfo.getFieldFormatInfos())
                .map(formatInfo -> FieldToRowDataConverters.createConverter(
                        TableFormatForRowDataUtils.deriveLogicalType(formatInfo)))
                .toArray(FieldToRowDataConverters.FieldToRowDataConverter[]::new);
    }

    @Override
    public RowData deserializeInternal(byte[] bytes) throws Exception {
        String text = new String(bytes, Charset.forName(charset));
        GenericRowData rowData = null;
        try {
            List<Map<String, String>> fieldTexts =
                    splitKv(text, entryDelimiter, kvDelimiter, escapeChar, quoteChar, null,
                            true);

            String[] fieldNames = rowFormatInfo.getFieldNames();
            FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

            rowData = new GenericRowData(fieldFormatInfos.length);
            for (int i = 0; i < fieldFormatInfos.length; i++) {
                String fieldName = fieldNames[i];
                FormatInfo fieldFormatInfo = fieldFormatInfos[i];

                String fieldText = fieldTexts.get(0).get(fieldName);

                Object field = deserializeBasicField(
                        fieldName,
                        fieldFormatInfo,
                        fieldText,
                        nullLiteral, failureHandler);
                rowData.setField(i, converters[i].convert(field));
            }
            return rowData;
        } catch (Throwable t) {
            failureHandler.onParsingMsgFailure(text, new RuntimeException(
                    String.format("Could not properly deserialize kv. Text=[{}].", text), t));
        }
        return null;
    }

    @Override
    public FormatMsg deserializeFormatMsg(byte[] bytes) throws Exception {
        String text = new String(bytes, Charset.forName(charset));
        GenericRowData rowData = null;
        long rowDataLength = 0L;
        try {
            List<Map<String, String>> fieldTexts =
                    splitKv(text, entryDelimiter, kvDelimiter, escapeChar, quoteChar, null,
                            true);

            String[] fieldNames = rowFormatInfo.getFieldNames();
            FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

            rowData = new GenericRowData(fieldFormatInfos.length);
            for (int i = 0; i < fieldFormatInfos.length; i++) {
                String fieldName = fieldNames[i];
                FormatInfo fieldFormatInfo = fieldFormatInfos[i];

                String fieldText = fieldTexts.get(0).get(fieldName);

                Object field = deserializeBasicField(
                        fieldName,
                        fieldFormatInfo,
                        fieldText,
                        nullLiteral, failureHandler);
                rowData.setField(i, converters[i].convert(field));
                rowDataLength += getFormatValueLength(fieldFormatInfo, fieldText);
            }
            return new FormatMsg(rowData, rowDataLength);
        } catch (Throwable t) {
            failureHandler.onParsingMsgFailure(text, new RuntimeException(
                    String.format("Could not properly deserialize kv. Text=[{}].", text), t));
        }
        return null;
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    /**
     * Builder for {@link KvRowDataDeserializationSchema}.
     */
    public static class Builder extends KvFormatBuilder<Builder> {

        private final TypeInformation<RowData> producedTypeInfo;
        public Builder(RowFormatInfo rowFormatInfo, TypeInformation<RowData> producedTypeInfo) {
            super(rowFormatInfo);
            this.producedTypeInfo = producedTypeInfo;
        }

        public KvRowDataDeserializationSchema build() {
            if (failureHandler != null) {
                return new KvRowDataDeserializationSchema(
                        rowFormatInfo,
                        producedTypeInfo,
                        charset,
                        entryDelimiter,
                        kvDelimiter,
                        escapeChar,
                        quoteChar,
                        nullLiteral, failureHandler);
            }
            return new KvRowDataDeserializationSchema(
                    rowFormatInfo,
                    producedTypeInfo,
                    charset,
                    entryDelimiter,
                    kvDelimiter,
                    escapeChar,
                    quoteChar,
                    nullLiteral);
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
        KvRowDataDeserializationSchema that = (KvRowDataDeserializationSchema) o;
        return rowFormatInfo.equals(that.rowFormatInfo) &&
                charset.equals(that.charset) &&
                entryDelimiter.equals(that.entryDelimiter) &&
                kvDelimiter.equals(that.kvDelimiter) &&
                Objects.equals(escapeChar, that.escapeChar) &&
                Objects.equals(quoteChar, that.quoteChar) &&
                Objects.equals(nullLiteral, that.nullLiteral) &&
                Objects.equals(failureHandler, that.failureHandler);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                rowFormatInfo,
                charset,
                entryDelimiter,
                kvDelimiter,
                escapeChar,
                quoteChar,
                nullLiteral, failureHandler);
    }
}
