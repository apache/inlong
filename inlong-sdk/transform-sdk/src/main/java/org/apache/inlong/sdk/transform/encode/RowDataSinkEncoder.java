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

package org.apache.inlong.sdk.transform.encode;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.RowDataSinkInfo;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sort.formats.base.FieldToRowDataConverters;
import org.apache.inlong.sort.formats.base.TableFormatUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowDataSinkEncoder extends SinkEncoder<RowData> {

    private final FieldToRowDataConverters.FieldToRowDataConverter[] fieldToRowDataConverters;
    private final Map<String, Integer> fieldPositionMap;

    public RowDataSinkEncoder(RowDataSinkInfo sinkInfo) {
        super(sinkInfo.getFields());
        this.fieldPositionMap = parseFieldPositionMap(fields);

        fieldToRowDataConverters = new FieldToRowDataConverters.FieldToRowDataConverter[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            fieldToRowDataConverters[i] = FieldToRowDataConverters.createConverter(
                    TableFormatUtils.deriveLogicalType(fields.get(i).getFormatInfo()));
        }
    }

    private Map<String, Integer> parseFieldPositionMap(List<FieldInfo> fields) {
        Map<String, Integer> map = new HashMap<>();
        for (int i =0; i < fields.size(); i++) {
            map.put(fields.get(i).getName(), i);
        }
        return map;
    }

    @Override
    public RowData encode(SinkData sinkData, Context context) {
        GenericRowData rowData = new GenericRowData(fieldToRowDataConverters.length);

        for (int i = 0; i < fields.size(); i++) {
            String fieldName = fields.get(i).getName();
            String fieldValue = sinkData.getField(fieldName);
            rowData.setField(i, fieldToRowDataConverters[i].convert(fieldValue));
        }

        return rowData;
    }
}
