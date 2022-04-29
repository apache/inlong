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

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.query.ck.ClickHouseColumnQueryBean;
import org.apache.inlong.manager.common.pojo.query.ck.ClickHouseTableQueryBean;

import java.util.ArrayList;
import java.util.List;

public class ClickHouseCreateTableSqlBuilder extends SqlBuilder<ClickHouseTableQueryBean> {

    @Override
    public String buildDDL(ClickHouseTableQueryBean table) {
        // Support _ beginning with underscore
        String dbTableName = table.getDbName() + "." + table.getTableName();
        ddl.append("CREATE TABLE ").append(dbTableName);
        // Construct columns and partition columns
        ddl.append(this.buildColumnsAndComments(table.getColumns()));
        // Set TERMINATED symbol
        if (StringUtils.isNotEmpty(table.getTableEngine())) { // set table comment
            ddl.append(" ENGINE = ").append(table.getTableEngine());
        } else {
            ddl.append(" ENGINE = Log");
        }
        if (StringUtils.isNotEmpty(table.getPartitionBy())) {
            ddl.append(" PARTITION BY ").append(table.getPartitionBy());
        }
        if (StringUtils.isNotEmpty(table.getOrderBy())) {
            ddl.append(" ORDER BY ").append(table.getOrderBy());
        }
        if (StringUtils.isNotEmpty(table.getPrimaryKey())) {
            ddl.append(" PRIMARY KEY ").append(table.getPrimaryKey());
        }
        if (StringUtils.isNotEmpty(table.getTableDesc())) {
            ddl.append(" COMMENT '").append(table.getTableDesc()).append("'");
        }
        return ddl.toString();
    }

    // (col_name data_type [COMMENT col_comment], col_name data_type [COMMENT col_comment]....)
    private String buildColumnsAndComments(List<ClickHouseColumnQueryBean> columns) {
        List<String> columnInfoList = new ArrayList<>();
        for (ClickHouseColumnQueryBean columnBean : columns) {
            // Construct columns and partition columns
            String columnName = columnBean.getColumnName();
            StringBuilder columnInfo = new StringBuilder().append(columnName).append(" ")
                    .append(columnBean.getColumnType());
            if (StringUtils.isNotEmpty(columnBean.getColumnDefaultType())) {
                columnInfo.append(" ").append(columnBean.getColumnDefaultType())
                        .append(" ").append(columnBean.getColumnDefaultExpr());
            }
            if (StringUtils.isNotEmpty(columnBean.getColumnCompressionCode())) {
                columnInfo.append(" CODEC(").append(columnBean.getColumnDesc()).append(")");
            }
            if (StringUtils.isNotEmpty(columnBean.getColumnTTLExpr())) {
                columnInfo.append(" TTL ").append(columnBean.getColumnTTLExpr());
            }
            if (StringUtils.isNotEmpty(columnBean.getColumnDesc())) {
                columnInfo.append(" COMMENT '").append(columnBean.getColumnDesc()).append("'");
            }
            columnInfoList.add(columnInfo.toString());
        }
        StringBuilder result = new StringBuilder().append("(").append(StringUtils.join(columnInfoList, ","))
                .append(")");
        return result.toString();
    }

    @Override
    public String getOPT() {
        return "CREATE_TABLE_CLICKHOUSE";
    }
}
