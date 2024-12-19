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

import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.RowDataSourceInfo;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.utils.RowToFieldDataUtils;
import org.apache.inlong.sort.formats.base.TableFormatForRowDataUtils;

import org.apache.flink.table.data.RowData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowDataSourceDecoder extends SourceDecoder<RowData> {

    private final Map<String, Integer> fieldPositionMap;
    private final RowToFieldDataUtils.RowFieldConverter[] rowFieldConverters;

    public RowDataSourceDecoder(RowDataSourceInfo sourceInfo) {
        super(sourceInfo.getFields());
        List<FieldInfo> fields = sourceInfo.getFields();
        this.fieldPositionMap = parseFieldPositionMap(fields);

        rowFieldConverters = new RowToFieldDataUtils.RowFieldConverter[fields.size()];
        for (int i = 0; i < rowFieldConverters.length; i++) {
            rowFieldConverters[i] = RowToFieldDataUtils.createNullableRowFieldConverter(
                    TableFormatForRowDataUtils.deriveLogicalType(fields.get(i).getFormatInfo()));
        }
    }

    private Map<String, Integer> parseFieldPositionMap(List<FieldInfo> fields) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            map.put(fields.get(i).getName(), i);
        }
        return map;
    }

    @Override
    public SourceData decode(byte[] srcBytes, Context context) {
        throw new UnsupportedOperationException("do not support decoding bytes for row data decoder");
    }

    @Override
    public SourceData decode(RowData rowData, Context context) {
        return new RowDataSourceData(rowData, fieldPositionMap, rowFieldConverters);
    }

}
