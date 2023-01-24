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

package org.apache.inlong.manager.service.resource.sink.kudu;

import org.apache.inlong.manager.pojo.sink.kudu.KuduColumnInfo;
import org.apache.inlong.manager.pojo.sink.kudu.KuduTableInfo;

import java.util.List;

public class KuduResourceClient {

    public KuduResourceClient(String metastoreUri, String dbName) {

    }

    public void open() {

    }

    public boolean tableExist(String tableName) {
        return false;
    }

    public void createTable(String tableName, KuduTableInfo tableInfo, boolean b) {

    }

    public List<KuduColumnInfo> getColumns(String dbName) {
        return null;
    }

    public void addColumns(String tableName, List<KuduColumnInfo> needAddColumns) {

    }

    public void close() {

    }
}
