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

package org.apache.inlong.sort.iceberg;

import org.apache.inlong.sort.base.Constants;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;

public enum IcebergWritableMetadata {

    AUDIT_DATA_TIME(
            Constants.META_AUDIT_DATA_TIME,
            DataTypes.BIGINT().notNull(),
            (r, p) -> r.getLong(p)),

    NULL(
            "",
            DataTypes.NULL(),
            (r, p) -> null);

    private final String key;
    private final DataType dataType;
    private final MetadataConverter converter;

    public String getKey() {
        return key;
    }

    public DataType getDataType() {
        return dataType;
    }

    public MetadataConverter getConverter() {
        return converter;
    }

    IcebergWritableMetadata(String key, DataType dataType, MetadataConverter converter) {
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
    }

    public interface MetadataConverter extends Serializable {

        Object read(RowData rowData, int pos);
    }
}
