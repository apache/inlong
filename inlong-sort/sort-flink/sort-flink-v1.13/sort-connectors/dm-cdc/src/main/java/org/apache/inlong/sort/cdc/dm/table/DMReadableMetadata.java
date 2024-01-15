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

package org.apache.inlong.sort.cdc.dm.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;

public enum DMReadableMetadata {

    /** Name of the database that contains the row. */
    DATABASE(
            "database_name",
            DataTypes.STRING().notNull(),
            new DMMetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(DMRecord record) {
                    return StringData.fromString(record.getSourceInfo().getDatabase());
                }
            }),

    /** Name of the database that contains the row. */
    SCHEMA(
            "schema_name",
            DataTypes.STRING().notNull(),
            new DMMetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(DMRecord record) {
                    return StringData.fromString(record.getSourceInfo().getSchema());
                }
            }),

    /** Name of the table that contains the row. */
    TABLE(
            "table_name",
            DataTypes.STRING().notNull(),
            new DMMetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(DMRecord record) {
                    return StringData.fromString(record.getSourceInfo().getTable());
                }
            }),

    /**
     * It indicates the scn that the change was made in the database. If the record is read from
     * snapshot of the table instead of the change stream, the value is always 0.
     */
    SCN(
            "scn",
            DataTypes.BIGINT().notNull(),
            new DMMetadataConverter() {

                private static final long serialVersionUID = 1L;

                @Override
                public Object read(DMRecord record) {
                    return record.getSourceInfo().getSCN();
                }
            });

    private final String key;

    private final DataType dataType;

    private final DMMetadataConverter converter;

    DMReadableMetadata(String key, DataType dataType, DMMetadataConverter converter) {
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
    }

    public String getKey() {
        return key;
    }

    public DataType getDataType() {
        return dataType;
    }

    public DMMetadataConverter getConverter() {
        return converter;
    }
}
