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

import org.apache.inlong.sort.base.format.AbstractDynamicSchemaFormat;
import org.apache.inlong.sort.kafka.SingleTablePrimaryKeyPartitioner;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * The unit tests for {@link
 * org.apache.inlong.sort.kafka.partitioner.SingleTablePrimaryKeyPartitioner}.
 */
public class SingleTablePrimaryKeyPartitionerTest {
    @Test
    public void testPrimaryKeyPartitioner() {
        SingleTablePrimaryKeyPartitioner singleTablePrimaryKeyPartitioner =
                new SingleTablePrimaryKeyPartitioner();

        TableSchema schema = new TableSchema

        singleTablePrimaryKeyPartitioner.setSchema(schema);
        singleTablePrimaryKeyPartitioner.setPartitionKey();
        singleTablePrimaryKeyPartitioner.setValueFieldGetters();

        int partition = singleTablePrimaryKeyPartitioner.partition(record, null,
                null, null, new int[]{0, 1, 2, 3});
        
        Assert.assertEquals(3, partition);
    }
}
