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

package org.apache.inlong.manager.service.thirdpart.hive.builder;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.query.hive.HiveColumnQueryBean;
import org.apache.inlong.manager.common.pojo.query.hive.HiveTableQueryBean;

public class HiveTableCreateSqlBuilder extends SqlBuilder<HiveTableQueryBean> {

    @Override
    public String buildDDL(HiveTableQueryBean table) {
        // Support _ beginning with underscore
        String dbTableName = "`" + table.getDbName() + "." + table.getTableName() + "`";
        ddl.append("CREATE TABLE ").append(dbTableName);
        // Construct columns and partition columns
        ddl.append(this.buildColumnsAndComments(table.getColumns(), table.getTableDesc()));
        // Set TERMINATED symbol
        if (table.getFieldTerSymbol() != null) {
            ddl.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '").append(table.getFieldTerSymbol()).append("'");
        }
        return ddl.toString();
    }

    // (col_name data_type [COMMENT col_comment], col_name data_type [COMMENT col_comment]....)
    private String buildColumnsAndComments(List<HiveColumnQueryBean> columns, String tableComment) {
        List<String> columnInfoList = new ArrayList<>();
        List<String> partitionList = new ArrayList<>();
        for (HiveColumnQueryBean columnBean : columns) {
            // Construct columns and partition columns
            String columnName = "`" + columnBean.getColumnName() + "`";
            StringBuilder columnInfo = new StringBuilder().append(columnName).append(" ")
                    .append(columnBean.getColumnType());
            if (StringUtils.isNotEmpty(columnBean.getColumnDesc())) { // comment is not empty
                columnInfo.append(" COMMENT ").append("'").append(columnBean.getColumnDesc()).append("'");
            }
            if (columnBean.isPartition()) { // partition field
                partitionList.add(columnInfo.toString());
            } else { // hive field
                columnInfoList.add(columnInfo.toString());
            }
        }
        StringBuilder result = new StringBuilder().append(" (").append(StringUtils.join(columnInfoList, ","))
                .append(") ");
        if (StringUtils.isNotEmpty(tableComment)) { // set table comment
            result.append("COMMENT ").append("'").append(tableComment).append("' ");
        }
        if (partitionList.size() > 0) { // set partitions
            result.append("PARTITIONED BY (").append(StringUtils.join(partitionList, ",")).append(") ");
        }
        return result.toString();
    }

    @Override
    public String getOPT() {
        return "CREATE_TABLE_HIVE";
    }

}
