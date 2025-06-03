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

package org.apache.inlong.sdk.transform.decode;

import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.utils.RowToFieldDataUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.data.RowData;

import java.util.Map;

@Slf4j
public class RowDataSourceData extends AbstractSourceData {

    private final RowData rowData;
    private final Map<String, Integer> fieldPositionMap;
    private final RowToFieldDataUtils.RowFieldConverter[] converters;

    public RowDataSourceData(
            RowData rowData,
            Map<String, Integer> fieldPositionMap,
            RowToFieldDataUtils.RowFieldConverter[] converters, Context context) {
        this.rowData = rowData;
        this.fieldPositionMap = fieldPositionMap;
        this.converters = converters;
        this.context = context;
    }

    @Override
    public int getRowCount() {
        return 1;
    }

    @Override
    public Object getField(int rowNum, String fieldName) {
        if (rowNum != 0) {
            return null;
        }
        try {
            if (isContextField(fieldName)) {
                return getContextField(fieldName);
            }
            int fieldPosition = fieldPositionMap.get(fieldName);
            return converters[fieldPosition].convert(rowData, fieldPosition);
        } catch (Throwable e) {
            log.error("failed to convert field={}", fieldName, e);
            return null;
        }
    }

}
