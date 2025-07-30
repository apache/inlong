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

import org.apache.inlong.sort.clickhouse.source.ClickHouseOptions;
import org.apache.inlong.sort.clickhouse.source.ClickHouseRowDataInputFormat;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ClickHouseRowDataInputFormatTest {

    private ClickHouseRowDataInputFormat inputFormat;
    private Connection mockConnection;
    private PreparedStatement mockStatement;
    private ResultSet mockResultSet;

    @BeforeEach
    public void setup() throws Exception {
        RowType rowType = new RowType(
                java.util.Arrays.asList(
                        new RowType.RowField("id", new IntType()),
                        new RowType.RowField("name", new VarCharType())));

        org.apache.flink.configuration.ReadableConfig mockConfig =
                mock(org.apache.flink.configuration.ReadableConfig.class);
        when(mockConfig.get(ClickHouseOptions.URL)).thenReturn("jdbc:clickhouse://localhost:8123/default");
        when(mockConfig.get(ClickHouseOptions.USERNAME)).thenReturn("default_user");
        when(mockConfig.get(ClickHouseOptions.PASSWORD)).thenReturn("default_pass");
        when(mockConfig.get(ClickHouseOptions.QUERY)).thenReturn("SELECT id, name FROM test_table");

        inputFormat = new ClickHouseRowDataInputFormat(mockConfig, rowType);

        mockConnection = mock(Connection.class);
        mockStatement = mock(PreparedStatement.class);
        mockResultSet = mock(ResultSet.class);

        java.lang.reflect.Field connField = ClickHouseRowDataInputFormat.class.getDeclaredField("connection");
        connField.setAccessible(true);
        connField.set(inputFormat, mockConnection);

        java.lang.reflect.Field stmtField = ClickHouseRowDataInputFormat.class.getDeclaredField("statement");
        stmtField.setAccessible(true);
        stmtField.set(inputFormat, mockStatement);

        java.lang.reflect.Field rsField = ClickHouseRowDataInputFormat.class.getDeclaredField("resultSet");
        rsField.setAccessible(true);
        rsField.set(inputFormat, mockResultSet);
    }

    @Test
    public void testReachedEnd() throws Exception {
        when(mockResultSet.next()).thenReturn(true).thenReturn(false);
        assertFalse(inputFormat.reachedEnd());
        assertTrue(inputFormat.reachedEnd());
    }

    @Test
    public void testNextRecord() throws Exception {
        when(mockResultSet.getObject(1)).thenReturn(1);
        when(mockResultSet.getObject(2)).thenReturn("test_name");
        when(mockResultSet.next()).thenReturn(true);

        RowData rowData = inputFormat.nextRecord(null);
        assertNotNull(rowData);
        assertEquals(2, rowData.getArity());
    }
}
