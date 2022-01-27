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

package org.apache.inlong.sort.flink.clickhouse.executor;

import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.flink.clickhouse.ClickHouseStatementFactory;
import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo;
import java.util.Optional;

public class ClickHouseExecutorFactory {
    public static ClickHouseExecutor generateClickHouseExecutor(
            String tableName,
            String[] fieldNames,
            FormatInfo[] formatInfos,
            ClickHouseSinkInfo clickHouseSinkInfo) {
        String[] keyFields = clickHouseSinkInfo.getKeyFieldNames();
        if (keyFields.length > 0) {
            String insertSql = ClickHouseStatementFactory.getInsertIntoStatement(tableName, fieldNames);
            String updateSql = ClickHouseStatementFactory.getUpdateStatement(
                    tableName, fieldNames, keyFields, Optional.empty());
            String deleteSql = ClickHouseStatementFactory.getDeleteStatement(tableName, keyFields, Optional.empty());
            return new ClickHouseUpsertExecutor(insertSql, updateSql, deleteSql, formatInfos, clickHouseSinkInfo);
        }

        String insertSql = ClickHouseStatementFactory.getInsertIntoStatement(tableName, fieldNames);
        return new ClickHouseAppendExecutor(insertSql, formatInfos, clickHouseSinkInfo);
    }
}
