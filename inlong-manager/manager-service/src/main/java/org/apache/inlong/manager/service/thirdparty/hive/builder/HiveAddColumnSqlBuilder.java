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

package org.apache.inlong.manager.service.thirdparty.hive.builder;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.query.hive.HiveColumnQueryBean;
import org.apache.inlong.manager.common.pojo.query.hive.HiveTableQueryBean;

public class HiveAddColumnSqlBuilder extends SqlBuilder<HiveTableQueryBean> {

    @Override
    public String buildDDL(HiveTableQueryBean table) {
        // Support _ beginning with underscore
        String dbTableName = "`" + table.getDbName() + "." + table.getTableName() + "`";
        ddl.append("ALTER TABLE ").append(dbTableName);
        ddl.append(this.buildColumns(table.getColumns()));
        return ddl.toString();
    }

    private String buildColumns(List<HiveColumnQueryBean> columns) {
        List<String> columnInfoList = new ArrayList<>();
        for (HiveColumnQueryBean columnBean : columns) {
            if (columnBean.isPartition()) {
                continue;
            }
            // Support _ beginning with underscore
            String columnName = "`" + columnBean.getColumnName() + "`";
            StringBuilder columnInfo = new StringBuilder().append(columnName).append(" ")
                    .append(columnBean.getColumnType());
            if (StringUtils.isNotEmpty(columnBean.getColumnDesc())) { // comment is not empty
                columnInfo.append(" COMMENT ").append("'").append(columnBean.getColumnDesc()).append("'");
            }
            columnInfoList.add(columnInfo.toString());
        }
        return " ADD COLUMNS (" + StringUtils.join(columnInfoList, ",") + ") ";
    }

    @Override
    public String getOPT() {
        return null;
    }

}
