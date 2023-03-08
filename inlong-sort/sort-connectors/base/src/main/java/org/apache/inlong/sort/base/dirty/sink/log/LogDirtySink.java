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

package org.apache.inlong.sort.base.dirty.sink.log;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonOptions.MapNullKeyMode;
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.formats.json.RowDataToJsonConverters.RowDataToJsonConverter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.inlong.sort.base.dirty.DirtyData;
import org.apache.inlong.sort.base.dirty.sink.DirtySink;
import org.apache.inlong.sort.base.dirty.utils.FormatUtils;
import org.apache.inlong.sort.base.util.LabelUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Log dirty sink that is used to print log
 *
 * @param <T>
 */
public class LogDirtySink<T> implements DirtySink<T> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(LogDirtySink.class);
    private final String format;
    private final String fieldDelimiter;
    private final DataType physicalRowDataType;
    private RowData.FieldGetter[] fieldGetters;
    private RowDataToJsonConverter converter;

    public LogDirtySink(String format, String fieldDelimiter, DataType physicalRowDataType) {
        this.format = format;
        this.fieldDelimiter = fieldDelimiter;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        converter = new RowDataToJsonConverters(TimestampFormat.SQL, MapNullKeyMode.DROP, null)
                .createConverter(physicalRowDataType.getLogicalType());
        fieldGetters = FormatUtils.parseFieldGetters(physicalRowDataType.getLogicalType());
    }

    @Override
    public void invoke(DirtyData<T> dirtyData) throws Exception {
        String value;
        Map<String, String> labelMap = LabelUtils.parseLabels(dirtyData.getLabels());
        T data = dirtyData.getData();
        if (data instanceof RowData) {
            value = format((RowData) data, dirtyData.getRowType(), labelMap);
        } else if (data instanceof JsonNode) {
            value = format((JsonNode) data, labelMap);
        } else {
            // Only support csv format when the row is not a 'RowData' and 'JsonNode'
            value = FormatUtils.csvFormat(data, labelMap, fieldDelimiter);
        }
        LOGGER.debug("[{}] {}", dirtyData.getLogTag(), value);
    }

    private String format(RowData data, LogicalType rowType,
            Map<String, String> labels) throws JsonProcessingException {
        String value;
        switch (format) {
            case "csv":
                RowData.FieldGetter[] getters = fieldGetters;
                if (rowType != null) {
                    getters = FormatUtils.parseFieldGetters(rowType);
                }
                value = FormatUtils.csvFormat(data, getters, labels, fieldDelimiter);
                break;
            case "json":
                RowDataToJsonConverter jsonConverter = converter;
                if (rowType != null) {
                    jsonConverter = FormatUtils.parseRowDataToJsonConverter(rowType);
                }
                value = FormatUtils.jsonFormat(data, jsonConverter, labels);
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported format for: %s", format));
        }
        return value;
    }

    private String format(JsonNode data, Map<String, String> labels) throws JsonProcessingException {
        String value;
        switch (format) {
            case "csv":
                value = FormatUtils.csvFormat(data, labels, fieldDelimiter);
                break;
            case "json":
                value = FormatUtils.jsonFormat(data, labels);
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported format for: %s", format));
        }
        return value;
    }
}
