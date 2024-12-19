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

import org.apache.inlong.sdk.transform.pojo.RowDataSinkInfo;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.utils.FieldToRowDataUtils;
import org.apache.inlong.sort.formats.base.TableFormatUtils;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

public class RowDataSinkEncoder extends SinkEncoder<RowData> {

    private final FieldToRowDataUtils.FieldToRowDataConverter[] fieldToRowDataConverters;

    public RowDataSinkEncoder(RowDataSinkInfo sinkInfo) {
        super(sinkInfo.getFields());

        fieldToRowDataConverters = new FieldToRowDataUtils.FieldToRowDataConverter[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            fieldToRowDataConverters[i] = FieldToRowDataUtils.createConverter(
                    TableFormatUtils.deriveLogicalType(fields.get(i).getFormatInfo()));
        }
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
