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

package org.apache.inlong.sort.iceberg.source.reader;

import org.apache.inlong.sort.iceberg.IcebergReadableMetadata.MetadataConverter;
import org.apache.inlong.sort.iceberg.utils.RecyclableJoinedRowData;
import org.apache.inlong.sort.iceberg.utils.RowDataCloneUtil;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.RowType;

/**
 * Copy from iceberg-flink:iceberg-flink-1.15:1.3.1
 */
class RowDataRecordFactory implements RecordFactory<RowData> {

    private final RowType rowType;
    private final TypeSerializer[] fieldSerializers;
    private final MetadataConverter[] metadataConverters;

    RowDataRecordFactory(RowType rowType, MetadataConverter[] metadataConverters) {
        this.rowType = rowType;
        this.fieldSerializers = createFieldSerializers(rowType);
        this.metadataConverters = metadataConverters;
    }

    static TypeSerializer[] createFieldSerializers(RowType rowType) {
        return rowType.getChildren().stream()
                .map(InternalSerializers::create)
                .toArray(TypeSerializer[]::new);
    }

    @Override
    public RowData[] createBatch(int batchSize) {
        RowData[] arr = new RowData[batchSize];
        for (int i = 0; i < batchSize; ++i) {
            arr[i] = new RecyclableJoinedRowData(rowType.getFieldCount(), metadataConverters.length);
        }
        return arr;
    }

    @Override
    public void clone(RowData from, RowData[] batch, int position) {

        RecyclableJoinedRowData recyclable;
        if (batch[position] instanceof RecyclableJoinedRowData) {
            recyclable = (RecyclableJoinedRowData) batch[position];
        } else {
            recyclable = new RecyclableJoinedRowData(rowType.getFieldCount(), metadataConverters.length);
        }

        RowData physical =
                RowDataCloneUtil.clonePhysical(from, recyclable.getPhysicalRowData(), rowType, fieldSerializers);
        RowData meta = RowDataCloneUtil.cloneMeta(from, recyclable.getMetaRowData(), metadataConverters);
        batch[position] = recyclable.replace(physical.getRowKind(), physical, meta, System.currentTimeMillis());
    }
}
