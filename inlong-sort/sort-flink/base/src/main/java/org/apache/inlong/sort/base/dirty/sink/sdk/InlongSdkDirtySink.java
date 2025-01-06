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

package org.apache.inlong.sort.base.dirty.sink.sdk;

import org.apache.inlong.sdk.dirtydata.DirtyMessageWrapper;
import org.apache.inlong.sdk.dirtydata.InlongSdkDirtySender;
import org.apache.inlong.sort.base.dirty.DirtyData;
import org.apache.inlong.sort.base.dirty.sink.DirtySink;
import org.apache.inlong.sort.base.dirty.utils.FormatUtils;
import org.apache.inlong.sort.base.util.LabelUtils;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Base64;
import java.util.Map;

@Slf4j
public class InlongSdkDirtySink<T> implements DirtySink<T> {

    private final InlongSdkDirtyOptions options;
    private final DataType physicalRowDataType;

    private transient RowData.FieldGetter[] fieldGetters;
    private transient RowDataToJsonConverters.RowDataToJsonConverter converter;
    private transient InlongSdkDirtySender dirtySender;

    public InlongSdkDirtySink(InlongSdkDirtyOptions options, DataType physicalRowDataType) {
        this.options = options;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public void invoke(DirtyData<T> dirtyData) throws Exception {
        try {
            Map<String, String> labelMap = LabelUtils.parseLabels(dirtyData.getLabels());
            String dataGroupId = Preconditions.checkNotNull(labelMap.get("groupId"));
            String dataStreamId = Preconditions.checkNotNull(labelMap.get("streamId"));
            String dataflowId = Preconditions.checkNotNull(labelMap.get("dataflowId"));

            String dirtyMessage = formatData(dirtyData, labelMap);

            DirtyMessageWrapper wrapper = DirtyMessageWrapper.builder()
                    .delimiter(options.getCsvFieldDelimiter())
                    .inlongGroupId(dataGroupId)
                    .inlongStreamId(dataStreamId)
                    .dataflowId(dataflowId)
                    .dataTime(dirtyData.getDataTime())
                    .serverType(dirtyData.getServerType())
                    .dirtyType(dirtyData.getDirtyType().format())
                    .dirtyMessage(dirtyData.getDirtyMessage())
                    .ext(dirtyData.getExtParams())
                    .data(dirtyMessage)
                    .build();

            dirtySender.sendDirtyMessageAsync(wrapper);
        } catch (Throwable t) {
            log.error("failed to send dirty message to inlong sdk", t);
            if (!options.isIgnoreSideOutputErrors()) {
                throw new RuntimeException("failed to send dirty message to inlong sdk", t);
            }
        }
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        converter = FormatUtils.parseRowDataToJsonConverter(physicalRowDataType.getLogicalType());
        fieldGetters = FormatUtils.parseFieldGetters(physicalRowDataType.getLogicalType());

        // init sender
        dirtySender = InlongSdkDirtySender.builder()
                .inlongManagerAddr(options.getInlongManagerAddr())
                .inlongManagerPort(options.getInlongManagerPort())
                .authId(options.getInlongManagerAuthId())
                .authKey(options.getInlongManagerAuthKey())
                .ignoreErrors(options.isIgnoreSideOutputErrors())
                .inlongGroupId(options.getSendToGroupId())
                .inlongStreamId(options.getSendToStreamId())
                .maxRetryTimes(options.getRetryTimes())
                .maxCallbackSize(options.getMaxCallbackSize())
                .build();
        dirtySender.init();
    }

    @Override
    public void close() throws Exception {
        if (dirtySender != null) {
            dirtySender.close();
        }
    }

    private String formatData(DirtyData<T> dirtyData, Map<String, String> labels) throws JsonProcessingException {
        String value;
        T data = dirtyData.getData();
        if (data instanceof RowData) {
            value = formatRowData((RowData) data, dirtyData.getRowType(), labels);
        } else if (data instanceof byte[]) {
            value = formatBytes((byte[]) data);
        } else {
            value = data.toString();
        }
        return value;
    }

    private String formatBytes(byte[] data) {
        return Base64.getEncoder().encodeToString(data);
    }

    private String formatRowData(RowData data, LogicalType rowType,
            Map<String, String> labels) throws JsonProcessingException {
        String value;
        switch (options.getFormat()) {
            case "csv":
                RowData.FieldGetter[] getters = fieldGetters;
                if (rowType != null) {
                    getters = FormatUtils.parseFieldGetters(rowType);
                }
                value = FormatUtils.csvFormat(data, getters, null, options.getCsvFieldDelimiter());
                break;
            case "json":
                RowDataToJsonConverters.RowDataToJsonConverter jsonConverter = converter;
                if (rowType != null) {
                    jsonConverter = FormatUtils.parseRowDataToJsonConverter(rowType);
                }
                value = FormatUtils.jsonFormat(data, jsonConverter, labels);
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported format for: %s", options.getFormat()));
        }
        return value;
    }
}
