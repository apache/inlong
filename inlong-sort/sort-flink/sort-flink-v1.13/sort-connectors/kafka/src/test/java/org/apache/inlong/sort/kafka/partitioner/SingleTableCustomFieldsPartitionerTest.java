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

package org.apache.inlong.sort.kafka.partitioner;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.types.logical.IntType;
import org.junit.Assert;
import org.junit.Test;

/**
 * The unit tests for {@link
 * org.apache.inlong.sort.kafka.partitioner.SingleTableCustomFieldsPartitioner}.
 */
public class SingleTableCustomFieldsPartitionerTest {

    @Test
    public void testCustomFieldsPartitioner() {
        SingleTableCustomFieldsPartitioner singleTableCustomFieldsPartitioner =
                new SingleTableCustomFieldsPartitioner();

        TableSchema schema = TableSchema.builder()
                .field("id", DataTypes.INT())
                .field("age", DataTypes.INT())
                .build();

        singleTableCustomFieldsPartitioner.setSchema(schema);
        singleTableCustomFieldsPartitioner.setPartitionKey("age");

        FieldGetter getter0 = RowData.createFieldGetter(new IntType(), 0);
        FieldGetter getter1 = RowData.createFieldGetter(new IntType(), 1);
        FieldGetter[] valuefieldgetters = {getter0, getter1};

        singleTableCustomFieldsPartitioner.setValueFieldGetters(valuefieldgetters);

        BinaryRowData rowData1 = new BinaryRowData(2);
        BinaryRowWriter writer1 = new BinaryRowWriter(rowData1);
        BinaryRowData rowData2 = new BinaryRowData(2);
        BinaryRowWriter writer2 = new BinaryRowWriter(rowData2);

        writer1.writeInt(0, 1);
        writer1.writeInt(1, 786819156);

        writer2.writeInt(0, 2);
        writer2.writeInt(1, 786819156);

        // key is null since the actual key is being calculated from deserialized json
        int partition1 = singleTableCustomFieldsPartitioner.partition(rowData1, null,
                null, null, new int[]{0, 1, 2, 3, 4});

        int partition2 = singleTableCustomFieldsPartitioner.partition(rowData2, null,
                null, null, new int[]{0, 1, 2, 3, 4});

        writer1.complete();
        writer2.complete();

        Assert.assertEquals(partition1, partition2);
    }
}
