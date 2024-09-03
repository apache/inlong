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

import org.apache.inlong.sdk.dataproxy.DefaultMessageSender;
import org.apache.inlong.sdk.dataproxy.MessageSender;
import org.apache.inlong.sdk.dataproxy.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.common.SendMessageCallback;
import org.apache.inlong.sdk.dataproxy.common.SendResult;
import org.apache.inlong.sort.base.dirty.DirtyData;
import org.apache.inlong.sort.base.dirty.DirtyType;
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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Map;
import java.util.StringJoiner;

@Slf4j
public class InlongSdkDirtySink<T> implements DirtySink<T> {

    private final InlongSdkOptions options;
    private final DataType physicalRowDataType;
    private final String inlongGroupId;
    private final String inlongStreamId;
    private final SendMessageCallback callback;

    private transient DateTimeFormatter dateTimeFormatter;
    private transient RowData.FieldGetter[] fieldGetters;
    private transient RowDataToJsonConverters.RowDataToJsonConverter converter;
    private transient MessageSender sender;

    public InlongSdkDirtySink(InlongSdkOptions options, DataType physicalRowDataType) {
        this.options = options;
        this.physicalRowDataType = physicalRowDataType;
        this.inlongGroupId = options.getInlongGroupId();
        this.inlongStreamId = options.getInlongStreamId();
        this.callback = new LogCallBack();
    }

    @Override
    public void invoke(DirtyData<T> dirtyData) throws Exception {
        try {
            Map<String, String> labelMap = LabelUtils.parseLabels(dirtyData.getLabels());
            String groupId = Preconditions.checkNotNull(labelMap.get("groupId"));
            String streamId = Preconditions.checkNotNull(labelMap.get("streamId"));

            String message = join(groupId, streamId,
                    dirtyData.getDirtyType(), dirtyData.getLabels(), formatData(dirtyData, labelMap));
            sender.asyncSendMessage(inlongGroupId, inlongStreamId, message.getBytes(), callback);
        } catch (Throwable t) {
            log.error("failed to send dirty message to inlong sdk", t);
        }
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        converter = FormatUtils.parseRowDataToJsonConverter(physicalRowDataType.getLogicalType());
        fieldGetters = FormatUtils.parseFieldGetters(physicalRowDataType.getLogicalType());
        dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // init sender
        ProxyClientConfig proxyClientConfig =
                new ProxyClientConfig(options.getInlongManagerAddr(), options.getInlongGroupId(),
                        options.getInlongManagerAuthId(), options.getInlongManagerAuthKey());
        sender = DefaultMessageSender.generateSenderByClusterId(proxyClientConfig);
    }

    @Override
    public void close() throws Exception {
        if (sender != null) {
            sender.close();
        }
    }

    private String join(
            String inlongGroup,
            String inlongStream,
            DirtyType type,
            String label,
            String formattedData) {

        String now = LocalDateTime.now().format(dateTimeFormatter);

        StringJoiner joiner = new StringJoiner(options.getCsvFieldDelimiter());
        return joiner.add(inlongGroup + "." + inlongStream)
                .add(now)
                .add(type.name())
                .add(label)
                .add(formattedData).toString();
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

    class LogCallBack implements SendMessageCallback {

        @Override
        public void onMessageAck(SendResult result) {
            if (result == SendResult.OK) {
                return;
            }
            log.error("failed to send inlong dirty message, response={}", result);

            if (!options.isIgnoreSideOutputErrors()) {
                throw new RuntimeException("writing dirty message to inlong sdk failed, response=" + result);
            }
        }

        @Override
        public void onException(Throwable e) {
            log.error("failed to send inlong dirty message", e);

            if (!options.isIgnoreSideOutputErrors()) {
                throw new RuntimeException("writing dirty message to inlong sdk failed", e);
            }
        }
    }
}
