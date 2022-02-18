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
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.sort.flink.hive.formats.orc.RowVectorizer.constructColumnVectorFromArray;
import static org.apache.inlong.sort.flink.hive.formats.orc.RowVectorizer.setColumn;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RowVectorizerTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testConstructColumnVectorFromArray() {
        Integer[] testInput = new Integer[] {1, 2, 3, 4, 5, 6};
        ColumnVector outputVector = constructColumnVectorFromArray(testInput, new IntType());
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

    @Test
    public void testWriteFile() throws IOException {
        String struct = "struct<test_array:array<int>,test_map:map<string,double>>";
        String testFilePath = temporaryFolder.newFolder().toString() + "/test.orc";

        TypeDescription schema = TypeDescription.fromString(struct);
        Configuration hadoopConf = new Configuration();
        try (Writer writer = OrcFile.createWriter(new Path(testFilePath),
                OrcFile.writerOptions(hadoopConf).setSchema(schema))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            List<int[]> intList = prepareListDataForOrcFileTest();
            List<Map<String, Double>> mapList = prepareMapDataForOrcFileTest();
            for (int i = 0; i < 5; i++) {
                batch.size++;
                setColumn(i, batch.cols[0], new ArrayType(new IntType()), Row.of("string", intList.get(i)), 1);
                setColumn(i, batch.cols[1], new MapType(new CharType(), new DoubleType()),
                        Row.of("String", mapList.get(i)), 1);
            }

            writer.addRowBatch(batch);
        }

        List<String> resultForList = new ArrayList<>();
        List<String> resultForMap = new ArrayList<>();
        try (Reader reader = OrcFile.createReader(new Path(testFilePath), OrcFile.readerOptions(hadoopConf))) {
            try (RecordReader records = reader.rows(reader.options())) {
                VectorizedRowBatch batch = reader.getSchema().createRowBatch(1024);
                ListColumnVector listColumnVector = (ListColumnVector) batch.cols[0];
                MapColumnVector mapColumnVector = (MapColumnVector) batch.cols[1];

                while (records.nextBatch(batch)) {
                    for (int rowNum = 0; rowNum < batch.size; rowNum++) {
                        StringBuilder stringBuilderForList = new StringBuilder();
                        listColumnVector.stringifyValue(stringBuilderForList, rowNum);
                        resultForList.add(stringBuilderForList.toString());

                        StringBuilder stringBuilderForMap = new StringBuilder();
                        mapColumnVector.stringifyValue(stringBuilderForMap, rowNum);
                        resultForMap.add(stringBuilderForMap.toString());
                    }
                }
            }
        }

        List<String> expectedForList = new ArrayList<>();
        expectedForList.add("[1, 2, 3]");
        expectedForList.add("null");
        expectedForList.add("[6, 5, 4]");
        expectedForList.add("null");
        expectedForList.add("[7, 8, 9, 10]");

        List<String> expectedForMap = new ArrayList<>();
        expectedForMap.add("[{\"key\": \"mary\", \"value\": 100.0}, {\"key\": \"lisa\", \"value\": 95.0}, {\"key\": "
                + "\"anna\", \"value\": 99.0}]");
        expectedForMap.add("null");
        expectedForMap.add("null");
        expectedForMap.add("[{\"key\": \"mary4\", \"value\": 100.4}, {\"key\": \"lisa4\", \"value\": 95.4}, {\"key\":"
                + " \"anna4\", \"value\": 99.4}]");
        expectedForMap.add("[{\"key\": \"luna5\", \"value\": 98.5}, {\"key\": \"mary5\", \"value\": 100.5}, "
                + "{\"key\": \"lili5\", \"value\": 97.5}, {\"key\": \"lisa5\", \"value\": 95.5}, {\"key\": \"anna5\","
                + " \"value\": 99.5}]");

        assertEquals(expectedForList, resultForList);
        assertEquals(expectedForMap, resultForMap);
    }

    private List<int[]> prepareListDataForOrcFileTest() {
        List<int[]> data = new ArrayList<>();
        int[] data1 = new int[] {1, 2, 3};
        data.add(data1);

        int[] data2 = new int[] {};
        data.add(data2);

        int[] data3 = new int[] {6, 5, 4};
        data.add(data3);

        data.add(null);

        int[] data5 = new int[] {7, 8, 9, 10};
        data.add(data5);

        return data;
    }

    private List<Map<String, Double>> prepareMapDataForOrcFileTest() {

        Map<String, Double> data1 = new HashMap<>();
        data1.put("anna", 99.0);
        data1.put("lisa", 95.0);
        data1.put("mary", 100.0);
        List<Map<String, Double>> data = new ArrayList<>();
        data.add(data1);

        Map<String, Double> data2 = new HashMap<>();
        data.add(data2);

        data.add(null);

        Map<String, Double> data4 = new HashMap<>();
        data4.put("anna4", 99.4);
        data4.put("lisa4", 95.4);
        data4.put("mary4", 100.4);
        data.add(data4);

        Map<String, Double> data5 = new HashMap<>();
        data5.put("anna5", 99.5);
        data5.put("lisa5", 95.5);
        data5.put("mary5", 100.5);
        data5.put("luna5", 98.5);
        data5.put("lili5", 97.5);
        data.add(data5);

        return data;
    }
}
