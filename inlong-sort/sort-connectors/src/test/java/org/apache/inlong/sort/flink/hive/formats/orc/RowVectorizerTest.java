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

package org.apache.inlong.sort.flink.hive.formats.orc;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.types.Row;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.inlong.sort.flink.hive.formats.orc.RowVectorizer.constructColumnVectorFromArray;
import static org.apache.inlong.sort.flink.hive.formats.orc.RowVectorizer.setColumn;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RowVectorizerTest {

    @Test
    public void testConstructColumnVectorFromArray() {
        Integer[] testInput = new Integer[] {1, 2, 3, 4, 5, 6};
        ColumnVector outputVector = constructColumnVectorFromArray(testInput);
        assertTrue(outputVector instanceof LongColumnVector);

        LongColumnVector longColumnVector = (LongColumnVector) outputVector;
        assertArrayEquals(new long[] {1, 2, 3, 4, 5, 6}, longColumnVector.vector);
    }

    @Test
    public void testSetColumnForArray() {
        LongColumnVector innerVector = new LongColumnVector(3);
        ListColumnVector testVector = new ListColumnVector(1, innerVector);
        Row testRow = Row.of("String", new Integer[] {1, 2, 3});
        setColumn(0, testVector, new ArrayType(new IntType()), testRow, 1);
        assertEquals(3, testVector.lengths[0]);

        StringBuilder stringBuilder = new StringBuilder();
        testVector.stringifyValue(stringBuilder, 0);
        assertEquals("[1, 2, 3]", stringBuilder.toString());
    }

    @Test
    public void testSetColumnForMap() {
        Map<Integer, Double> testMap = new HashMap<>();
        testMap.put(1, 10.0);
        testMap.put(2, 20.0);
        testMap.put(3, 30.0);
        Row testRow = Row.of(testMap);
        LongColumnVector keyVector = new LongColumnVector(3);
        DoubleColumnVector valueVector = new DoubleColumnVector(3);
        MapColumnVector testVector = new MapColumnVector(1, keyVector, valueVector);
        setColumn(0, testVector, new MapType(new IntType(), new DoubleType()), testRow, 0);
        assertEquals(3, testVector.lengths[0]);

        StringBuilder stringBuilder = new StringBuilder();
        testVector.stringifyValue(stringBuilder, 0);
        assertEquals(
                "[{\"key\": 1, \"value\": 10.0}, {\"key\": 2, \"value\": 20.0}, {\"key\": 3, \"value\": 30.0}]",
                stringBuilder.toString()
        );
    }

}
