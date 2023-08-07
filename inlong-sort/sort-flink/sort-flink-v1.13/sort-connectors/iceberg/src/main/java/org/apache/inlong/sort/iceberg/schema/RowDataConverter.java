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

package org.apache.inlong.sort.iceberg.schema;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;

import static org.apache.inlong.sort.base.Constants.SWITCH_APPEND_UPSERT_ENABLE;

/**
 * used in iceberg running in {@link SWITCH_APPEND_UPSERT_ENABLE}
 */
public class RowDataConverter {

    private final RowData.FieldGetter[] fieldGetter;

    public RowDataConverter(List<LogicalType> types) {
        this.fieldGetter = new RowData.FieldGetter[types.size()];

        for (int i = 0; i < types.size(); ++i) {
            this.fieldGetter[i] = RowData.createFieldGetter(types.get(i), i);
        }

    }

    public Object get(RowData struct, int index) {
        return this.fieldGetter[index].getFieldOrNull(struct);
    }

}
