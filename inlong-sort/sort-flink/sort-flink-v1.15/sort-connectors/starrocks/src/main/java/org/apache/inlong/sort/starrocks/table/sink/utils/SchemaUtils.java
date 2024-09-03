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

package org.apache.inlong.sort.starrocks.table.sink.utils;

import org.apache.flink.table.api.TableSchema;

import java.io.Serializable;
import java.util.Arrays;

/**
 * SchemaUtils for StarRocksDynamicTableSink
 * Deals with schema related operations such as finding the index of
 * special field in fieldNames and filter out special field in data.
 */
public class SchemaUtils implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String AUDIT_DATA_TIME = "AUDIT_DATA_TIME";
    private final int DATA_TIME_ABSENT_INDEX = -1;
    private final int dataTimeFieldIndex;

    public SchemaUtils(TableSchema schema) {
        this.dataTimeFieldIndex = getDataTimeIndex(schema.getFieldNames());
    }

    public long getDataTime(Object[] data) {
        if (dataTimeFieldIndex == DATA_TIME_ABSENT_INDEX) {
            // if AUDIT_DATA_TIME field is absent, return local time
            return System.currentTimeMillis();
        }
        return (Long) data[dataTimeFieldIndex];
    }

    /**
     * filter out AUDIT_DATA_TIME field
     * @param data
     * @return data without AUDIT_DATA_TIME
     */
    public Object[] filterOutTimeField(Object[] data) {
        if (dataTimeFieldIndex == DATA_TIME_ABSENT_INDEX) {
            return data;
        }
        Object[] filteredData = new Object[data.length - 1];
        for (int i = 0, j = 0; i < data.length; i++) {
            if (i != dataTimeFieldIndex) {
                filteredData[j++] = data[i];
            }
        }
        return filteredData;
    }

    /**
     * AUDIT_DATA_TIME should not occur in actual data schema fields
     *
     * @param schema
     * @return fieldNames without AUDIT_DATA_TIME
     */
    public String[] filterOutTimeField(TableSchema schema) {
        return Arrays.stream(schema.getFieldNames())
                .filter(field -> !AUDIT_DATA_TIME.equalsIgnoreCase(field))
                .toArray(String[]::new);
    }

    /**
     * get the index of AUDIT_DATA_TIME in fieldNames
     * @param fieldNames
     * @return index of AUDIT_DATA_TIME in fieldNames, or DATA_TIME_ABSENT_INDEX if absent
     */
    private int getDataTimeIndex(String[] fieldNames) {
        for (int i = 0; i < fieldNames.length; i++) {
            if (AUDIT_DATA_TIME.equalsIgnoreCase(fieldNames[i])) {
                return i;
            }
        }
        return DATA_TIME_ABSENT_INDEX;
    }

}
