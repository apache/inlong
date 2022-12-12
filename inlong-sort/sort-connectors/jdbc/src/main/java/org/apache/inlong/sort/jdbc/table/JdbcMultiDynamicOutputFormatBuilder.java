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

package org.apache.inlong.sort.jdbc.table;

import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.data.RowData;
import org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy;
import org.apache.inlong.sort.jdbc.internal.JdbcMultiBatchingOutputFormat;

import java.io.Serializable;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class JdbcMultiDynamicOutputFormatBuilder implements Serializable {

    private static final long serialVersionUID = 1L;
    private JdbcOptions jdbcOptions;
    private JdbcExecutionOptions executionOptions;
    private JdbcDmlOptions dmlOptions;
    private boolean appendMode;
    private String inlongMetric;
    private String auditHostAndPorts;
    private String sinkMultipleFormat;
    private String databasePattern;
    private String tablePattern;
    private String schemaPattern;
    private SchemaUpdateExceptionPolicy schemaUpdateExceptionPolicy;

    public JdbcMultiDynamicOutputFormatBuilder setJdbcOptions(JdbcOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
        return this;
    }

    public JdbcMultiDynamicOutputFormatBuilder setJdbcExecutionOptions(
            JdbcExecutionOptions executionOptions) {
        this.executionOptions = executionOptions;
        return this;
    }

    public JdbcMultiDynamicOutputFormatBuilder setJdbcDmlOptions(JdbcDmlOptions dmlOptions) {
        this.dmlOptions = dmlOptions;
        return this;
    }

    public JdbcMultiDynamicOutputFormatBuilder setAppendMode(boolean appendMode) {
        this.appendMode = appendMode;
        return this;
    }

    public JdbcMultiDynamicOutputFormatBuilder setInLongMetric(String inlongMetric) {
        this.inlongMetric = inlongMetric;
        return this;
    }

    public JdbcMultiDynamicOutputFormatBuilder setAuditHostAndPorts(String auditHostAndPorts) {
        this.auditHostAndPorts = auditHostAndPorts;
        return this;
    }

    public JdbcMultiDynamicOutputFormatBuilder setSinkMultipleFormat(String sinkMultipleFormat) {
        this.sinkMultipleFormat = sinkMultipleFormat;
        return this;
    }

    public JdbcMultiDynamicOutputFormatBuilder setDatabasePattern(String databasePattern) {
        this.databasePattern = databasePattern;
        return this;
    }

    public JdbcMultiDynamicOutputFormatBuilder setTablePattern(String tablePattern) {
        this.tablePattern = tablePattern;
        return this;
    }

    public JdbcMultiDynamicOutputFormatBuilder setSchemaPattern(String schemaPattern) {
        this.schemaPattern = schemaPattern;
        return this;
    }

    public JdbcMultiDynamicOutputFormatBuilder setSchemaUpdatePolicy(
            SchemaUpdateExceptionPolicy schemaUpdateExceptionPolicy) {
        this.schemaUpdateExceptionPolicy = schemaUpdateExceptionPolicy;
        return this;
    }

    public JdbcMultiDynamicOutputFormatBuilder() {

    }

    public JdbcMultiBatchingOutputFormat<RowData, ?, ?> build() {
        checkNotNull(jdbcOptions, "jdbc options can not be null");
        checkNotNull(dmlOptions, "jdbc dml options can not be null");
        checkNotNull(executionOptions, "jdbc execution options can not be null");
        return new JdbcMultiBatchingOutputFormat<>(
                new SimpleJdbcConnectionProvider(jdbcOptions),
                executionOptions,
                dmlOptions,
                appendMode,
                jdbcOptions,
                sinkMultipleFormat,
                databasePattern,
                tablePattern,
                schemaPattern,
                inlongMetric,
                auditHostAndPorts,
                schemaUpdateExceptionPolicy);
    }
}
