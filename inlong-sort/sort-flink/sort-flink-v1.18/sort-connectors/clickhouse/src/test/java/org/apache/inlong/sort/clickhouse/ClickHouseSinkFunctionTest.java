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

package org.apache.inlong.sort.clickhouse;

import org.apache.inlong.sort.clickhouse.source.ClickHouseSinkFunction;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;

import static org.mockito.Mockito.*;

public class ClickHouseSinkFunctionTest {

    private ClickHouseSinkFunction sinkFunction;
    private Connection mockConnection;
    private PreparedStatement mockStatement;

    @BeforeEach
    public void setup() throws Exception {
        RowType rowType = new RowType(
                java.util.Arrays.asList(
                        new RowType.RowField("id", new IntType()),
                        new RowType.RowField("name", new VarCharType())));

        sinkFunction = new ClickHouseSinkFunction(
                "jdbc:clickhouse://localhost:8123/default",
                "default_user",
                "default_pass",
                "test_table",
                rowType);

        mockConnection = mock(Connection.class);
        mockStatement = mock(PreparedStatement.class);

        java.lang.reflect.Field connField = ClickHouseSinkFunction.class.getDeclaredField("connection");
        connField.setAccessible(true);
        connField.set(sinkFunction, mockConnection);

        java.lang.reflect.Field stmtField = ClickHouseSinkFunction.class.getDeclaredField("statement");
        stmtField.setAccessible(true);
        stmtField.set(sinkFunction, mockStatement);
    }

    @Test
    public void testInvoke() throws Exception {
        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 123);
        rowData.setField(1, org.apache.flink.table.data.StringData.fromString("test_name"));
        rowData.setRowKind(RowKind.INSERT);
        SinkFunction.Context context = mock(SinkFunction.Context.class);
        sinkFunction.invoke(rowData, context);
        sinkFunction.flush();
        verify(mockStatement).executeBatch();
        verify(mockStatement, times(1)).setInt(1, 123);
        verify(mockStatement, times(1)).setString(2, "test_name");
        verify(mockStatement, times(1)).addBatch();
        verify(mockStatement, times(1)).executeBatch();
        verify(mockConnection, times(1)).commit();
    }

    @Test
    public void testClose() throws Exception {
        sinkFunction.close();
        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }
}
