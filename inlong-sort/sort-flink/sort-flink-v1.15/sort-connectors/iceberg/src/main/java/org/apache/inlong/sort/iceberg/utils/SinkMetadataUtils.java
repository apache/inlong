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

package org.apache.inlong.sort.iceberg.utils;

import org.apache.inlong.sort.base.Constants;
import org.apache.inlong.sort.iceberg.IcebergWritableMetadata;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.relocated.com.google.common.collect.BiMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableBiMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Use for parse sink metadata by pos or name.
 */
@Slf4j
public class SinkMetadataUtils implements Serializable {

    private static final long serialVersionUID = 1L;
    private final int DATA_TIME_INDEX;
    private final BiMap<String, Integer> field2posMap;
    private final BiMap<Integer, String> pos2Field;
    private final IcebergWritableMetadata.MetadataConverter[] converters;

    public SinkMetadataUtils(List<String> metadataKeys, DataType consumedDataType) {
        Set<String> metadataKeySet = ImmutableSet.copyOf(metadataKeys);
        List<RowType.RowField> metaFields = ((RowType) consumedDataType.getLogicalType()).getFields();
        List<String> names = metaFields.stream().map(RowType.RowField::getName).collect(Collectors.toList());
        log.info("start to config SinkMetadataUtils, metaKeys={}, consume fields={}", metadataKeys, names);
        // get related converters by real keys
        // the pos of physical column will be replaced by IcebergWritableMetadata.NULL
        this.converters = metaFields.stream()
                .map(RowType.RowField::getName)
                .map(key -> Stream.of(IcebergWritableMetadata.values())
                        .filter(m -> m.getKey().equalsIgnoreCase(key))
                        .findFirst()
                        .orElse(IcebergWritableMetadata.NULL))
                .map(IcebergWritableMetadata::getConverter)
                .toArray(IcebergWritableMetadata.MetadataConverter[]::new);

        // parse bimap of key name and pos
        ImmutableBiMap.Builder<String, Integer> builder = ImmutableBiMap.builder();
        for (int i = 0; i < metaFields.size(); i++) {
            String name = metaFields.get(i).getName();
            if (metadataKeySet.contains(name.toLowerCase())) {
                builder.put(name.toLowerCase(), i);
            }
        }
        this.field2posMap = builder.build();
        this.pos2Field = field2posMap.inverse();

        // for audit time
        DATA_TIME_INDEX = field2posMap.getOrDefault(Constants.META_AUDIT_DATA_TIME, -1);
        log.info("find data time index={}, filed2posMap={}", DATA_TIME_INDEX, field2posMap);
    }

    public Integer getMetadataPosByName(String name) {
        return field2posMap.get(name);
    }

    public String getMetadataNameByPos(Integer pos) {
        return pos2Field.get(pos);
    }

    public Object convertByName(RowData row, String name) {
        Integer pos = getMetadataPosByName(name);
        return pos == null ? null : converters[pos].read(row, pos);
    }

    public Object covertByPos(RowData row, int pos) {
        return pos < 0 ? null : converters[pos].read(row, pos);
    }

    public long getDataTime(Object data) {
        if (DATA_TIME_INDEX < 0 || !(data instanceof RowData)) {
            return System.currentTimeMillis();
        }
        return (Long) covertByPos((RowData) data, DATA_TIME_INDEX);
    }

    public int getDataSize(Object data) {
        return data.toString().getBytes(StandardCharsets.UTF_8).length;
    }

}
