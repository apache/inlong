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

import org.apache.inlong.sort.iceberg.IcebergReadableMetadata.MetadataConverter;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Support clone meta and physical RowData.
 */
public class RowDataCloneUtil {

    private RowDataCloneUtil() {
    }

    public static RowData cloneMeta(
            RowData from, RowData reuse, MetadataConverter[] converters) {
        GenericRowData ret;
        if (reuse instanceof GenericRowData) {
            ret = (GenericRowData) reuse;
        } else {
            ret = new GenericRowData(from.getArity());
        }

        for (int i = 0; i < converters.length; i++) {
            Object meta = converters[i].read(from);
            ret.setField(i, meta);
        }

        return ret;
    }

    public static RowData clonePhysical(
            RowData from, RowData reuse, RowType rowType, TypeSerializer[] fieldSerializers) {
        GenericRowData ret;
        if (reuse instanceof GenericRowData) {
            ret = (GenericRowData) reuse;
        } else {
            ret = new GenericRowData(from.getArity());
        }
        ret.setRowKind(from.getRowKind());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (!from.isNullAt(i)) {
                RowData.FieldGetter getter = RowData.createFieldGetter(rowType.getTypeAt(i), i);
                ret.setField(i, fieldSerializers[i].copy(getter.getFieldOrNull(from)));
            } else {
                ret.setField(i, null);
            }
        }
        return ret;
    }
}
