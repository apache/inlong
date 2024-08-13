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

package org.apache.inlong.sort.formats.inlongmsgbinlog;

import org.apache.inlong.sort.formats.inlongmsg.AbstractInLongMsgDecodingFormat;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import static org.apache.inlong.sort.formats.base.TableFormatOptions.ROW_FORMAT_INFO;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.deserializeRowFormatInfo;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.ATTRIBUTE_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.INCLUDE_UPDATE_BEFORE;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.METADATA_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.TIME_FIELD_NAME;

/**
 * InLongMsgBinlogDecodingFormat.
 */
public class InLongMsgBinlogDecodingFormat extends AbstractInLongMsgDecodingFormat {

    private final ReadableConfig formatOptions;

    public InLongMsgBinlogDecodingFormat(ReadableConfig formatOptions) {
        this.formatOptions = formatOptions;
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(
            DynamicTableSource.Context context,
            DataType dataType) {
        InLongMsgBinlogRowDataDeserializationSchema.Builder builder =
                new InLongMsgBinlogRowDataDeserializationSchema.Builder(
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
            InLongMsgBinlogRowDataDeserializationSchema.Builder schemaBuilder) {

        formatOptions
                .getOptional(TIME_FIELD_NAME)
                .ifPresent(schemaBuilder::setTimeFieldName);

        formatOptions
                .getOptional(ATTRIBUTE_FIELD_NAME)
                .ifPresent(schemaBuilder::setAttributesFieldName);

        formatOptions
                .getOptional(METADATA_FIELD_NAME)
                .ifPresent(schemaBuilder::setMetadataFieldName);

        formatOptions
                .getOptional(INCLUDE_UPDATE_BEFORE)
                .ifPresent(schemaBuilder::setIncludeUpdateBefore);

        formatOptions
                .getOptional(IGNORE_ERRORS)
                .ifPresent(schemaBuilder::setIgnoreErrors);
    }
}
