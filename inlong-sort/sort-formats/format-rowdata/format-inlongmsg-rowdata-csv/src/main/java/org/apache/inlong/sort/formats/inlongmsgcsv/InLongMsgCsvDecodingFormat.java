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

package org.apache.inlong.sort.formats.inlongmsgcsv;

import org.apache.inlong.sort.formats.inlongmsg.AbstractInLongMsgDecodingFormat;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import static org.apache.inlong.sort.formats.base.TableFormatOptions.ROW_FORMAT_INFO;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.deserializeRowFormatInfo;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.ATTRIBUTE_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.CHARSET;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.DELETE_HEAD_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.FIELD_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.LINE_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.NULL_LITERAL;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.QUOTE_CHARACTER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.TIME_FIELD_NAME;

/**
 * InLongMsgCsvDecodingFormat.
 */
public class InLongMsgCsvDecodingFormat extends AbstractInLongMsgDecodingFormat {

    private final ReadableConfig formatOptions;

    public InLongMsgCsvDecodingFormat(ReadableConfig formatOptions) {
        this.formatOptions = formatOptions;
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(
            DynamicTableSource.Context context, DataType dataType) {
        InLongMsgCsvRowDataDeserializationSchema.Builder builder =
                new InLongMsgCsvRowDataDeserializationSchema.Builder(
                        deserializeRowFormatInfo(formatOptions.get(ROW_FORMAT_INFO)));
        configureDeserializationSchema(formatOptions, builder);
        return builder.build();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    private void configureDeserializationSchema(
            ReadableConfig formatOptions,
            InLongMsgCsvRowDataDeserializationSchema.Builder schemaBuilder) {
        schemaBuilder.setCharset(formatOptions.getOptional(CHARSET).orElse(CHARSET.defaultValue()))
                .setMetadataKeys(metadataKeys);

        formatOptions
                .getOptional(TIME_FIELD_NAME)
                .ifPresent(schemaBuilder::setTimeFieldName);

        formatOptions
                .getOptional(ATTRIBUTE_FIELD_NAME)
                .ifPresent(schemaBuilder::setAttributesFieldName);

        formatOptions
                .getOptional(CHARSET)
                .ifPresent(schemaBuilder::setCharset);

        formatOptions
                .getOptional(LINE_DELIMITER)
                .map(delimiter -> StringEscapeUtils.unescapeJava(delimiter).charAt(0))
                .ifPresent(schemaBuilder::setLineDelimiter);

        formatOptions
                .getOptional(FIELD_DELIMITER)
                .map(delimiter -> StringEscapeUtils.unescapeJava(delimiter).charAt(0))
                .ifPresent(schemaBuilder::setFieldDelimiter);

        formatOptions
                .getOptional(QUOTE_CHARACTER)
                .map(quote -> quote.charAt(0))
                .ifPresent(schemaBuilder::setQuoteCharacter);

        formatOptions
                .getOptional(ESCAPE_CHARACTER)
                .map(escape -> escape.charAt(0))
                .ifPresent(schemaBuilder::setEscapeCharacter);

        formatOptions
                .getOptional(NULL_LITERAL)
                .ifPresent(schemaBuilder::setNullLiteral);

        formatOptions
                .getOptional(IGNORE_ERRORS)
                .ifPresent(schemaBuilder::setIgnoreErrors);

        formatOptions
                .getOptional(DELETE_HEAD_DELIMITER)
                .ifPresent(schemaBuilder::setDeleteHeadDelimiter);
    }
}
