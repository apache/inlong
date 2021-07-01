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

import org.apache.inlong.manager.common.pojo.query.hive.HiveTableQueryBean;

public class HiveQueryTableSqlBuilder extends SqlBuilder<HiveTableQueryBean> {

    @Override
    public String buildDDL(HiveTableQueryBean table) {
        String dbTableName = "`" + table.getDbName() + "." + table.getTableName() + "`";
        if (table.getColumns() == null || table.getColumns().size() == 0) {
            return null;
        }
        ddl.append("DESC ").append(dbTableName);
        return ddl.toString();
    }

    @Override
    public String getOPT() {
        return null;
    }
}
