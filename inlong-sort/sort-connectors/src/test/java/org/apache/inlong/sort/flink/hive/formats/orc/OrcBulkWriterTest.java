/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.flink.hive.formats.orc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.flink.hive.HiveSinkHelper;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HivePartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.OrcFileFormat;
import org.apache.inlong.sort.util.TestLogger;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.WriterImpl;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OrcBulkWriterTest extends TestLogger {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final Row testData = Row.of("wanger", 19);

    private static final WriterImpl mockWriter = mock(WriterImpl.class);

    private static final int rowBatchSize = 3;

    private static VectorizedRowBatch rowBatch;

    private static OrcBulkWriter orcBulkWriter;

    private static int colsExpected;

    @Before
    public void setup() {
        String[] fieldNames = new String[] {"name", "age"};
        LogicalType[] fieldTypes = new LogicalType[] {new VarCharType(), new IntType()};
        RowType rowType = RowType.of(fieldTypes, fieldNames);
        TypeDescription schema = OrcBulkWriterUtil.logicalTypeToOrcType(rowType);

        colsExpected = fieldNames.length;
        orcBulkWriter = new OrcBulkWriter(new RowVectorizer(schema, fieldTypes), mockWriter, rowBatchSize);
        rowBatch = orcBulkWriter.getVectorizedRowBatch();
    }

    @Test
    public void testAddElement() throws IOException {
        // add rowBatchSize-1 element(s)
        for (int i = 0; i < rowBatchSize - 1; i++) {
            orcBulkWriter.addElement(testData);
            verify(mockWriter, times(0)).addRowBatch(rowBatch);
        }
        assertEquals(colsExpected, rowBatch.cols.length);
        assertEquals(rowBatchSize - 1, rowBatch.size);

        // add one more element to triger rowBatch reset
        orcBulkWriter.addElement(testData);
        assertEquals(0, rowBatch.size);
        verify(mockWriter, times(1)).addRowBatch(rowBatch);
    }

    @Test
    public void testFlush() throws IOException {
        // no element, addRowBatch shouldn't be called
        orcBulkWriter.flush();
        verify(mockWriter, times(0)).addRowBatch(rowBatch);

        // add one element and then flush, addRowBatch should be called once and rowBatch should be reset
        orcBulkWriter.addElement(testData);
        orcBulkWriter.flush();
        verify(mockWriter, times(1)).addRowBatch(rowBatch);
        assertEquals(0, rowBatch.size);
    }

    @Test
    public void testFinish() throws IOException {
        orcBulkWriter.finish();
        verify(mockWriter, times(1)).close();
    }

    @Test
    public void testSchemaLengthBiggerThanRowLength() throws IOException {
        File testFile = temporaryFolder.newFile("test.orc");
        OrcBulkWriter orcBulkWriter = (OrcBulkWriter) HiveSinkHelper
                .createBulkWriterFactory(prepareHiveSinkInfo(), new Configuration())
                .create(new LocalDataOutputStream(testFile));

        orcBulkWriter.addElement(Row.of("name", 29, "male"));
        orcBulkWriter.finish();

        Reader reader = OrcFile.createReader(
                new Path(testFile.toURI()),
                OrcFile.readerOptions(new org.apache.hadoop.conf.Configuration()));
        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        while (rows.nextBatch(batch)) {
            assertEquals(1, batch.size);
            assertEquals(3, batch.cols.length);

            assertEquals("name", new String(((BytesColumnVector) batch.cols[0]).vector[0]));
            assertEquals(29, ((LongColumnVector) batch.cols[1]).vector[0]);
            assertEquals("male", new String(((BytesColumnVector) batch.cols[2]).vector[0]));
        }
        rows.close();
    }

    private HiveSinkInfo prepareHiveSinkInfo() {
        return new HiveSinkInfo(
                new FieldInfo[] {
                        new FieldInfo("f1", StringFormatInfo.INSTANCE),
                        new FieldInfo("f2", IntFormatInfo.INSTANCE),
                        new FieldInfo("f3", StringFormatInfo.INSTANCE),
                },
                "jdbc:mysql://127.0.0.1:3306/testDatabaseName",
                "testDatabaseName",
                "testTableName",
                "testUsername",
                "testPassword",
                "/path",
                new HivePartitionInfo[]{
                },
                new OrcFileFormat(64));
    }
}
