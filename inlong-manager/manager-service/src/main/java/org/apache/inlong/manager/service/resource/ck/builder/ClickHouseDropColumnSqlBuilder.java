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

package org.apache.inlong.manager.service.resource.ck.builder;

import org.apache.inlong.manager.common.pojo.query.ck.ClickHouseColumnQueryBean;
import org.apache.inlong.manager.common.pojo.query.ck.ClickHouseTableQueryBean;

import java.util.ArrayList;
import java.util.List;

public class ClickHouseDropColumnSqlBuilder extends SqlBuilder<ClickHouseTableQueryBean> {

    @Override
    public String buildDDL(ClickHouseTableQueryBean table) {
        String dbTableName = "`" + table.getDbName() + "." + table.getTableName() + "`";
        List<String> columnInfoList = this.buildColumns(table.getColumns());
        for (String columnInfo : columnInfoList) {
            ddl.append("ALTER TABLE ").append(dbTableName).append(" DROP COLUMN IF EXISTS")
                    .append(columnInfo).append("|");
        }
        return ddl.toString();
    }

    private List<String> buildColumns(List<ClickHouseColumnQueryBean> columns) {
        List<String> columnInfoList = new ArrayList<>();
        for (ClickHouseColumnQueryBean columnBean : columns) {
            // Support _ beginning with underscore
            String columnName = columnBean.getColumnName();
            StringBuilder columnInfo = new StringBuilder().append(columnName);
            columnInfoList.add(columnInfo.toString());
        }
        return columnInfoList;
    }

    @Override
    public String getOPT() {
        return "DROP_COLUMN_CLICKHOUSE";;
    }

}
