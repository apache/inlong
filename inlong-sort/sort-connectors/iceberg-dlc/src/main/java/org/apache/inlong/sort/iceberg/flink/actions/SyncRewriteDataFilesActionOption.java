/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.inlong.sort.iceberg.flink.actions;

import com.qcloud.dlc.common.Constants;
import org.apache.inlong.sort.iceberg.flink.FlinkCatalogFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.inlong.sort.iceberg.flink.FlinkDynamicTableFactory.CATALOG_DATABASE;
import static org.apache.inlong.sort.iceberg.flink.FlinkDynamicTableFactory.CATALOG_TABLE;

public class SyncRewriteDataFilesActionOption implements Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, String> properties;

    public static final String URL_REGION = "region";
    public static final String URL_REGION_DEFAULT = "ap-beijing";

    public static final String URL_DEFAULT_DATABASE = "database_name";
    public static final String URL_DEFAULT_DATABASE_DEFAULT = "default";

    public static final String URL_ENDPOINT = "endpoint";
    public static final String URL_ENDPOINT_DEFAULT = "dlc.tencentcloudapi.com";

    public static final String URL_TASK_TYPE = "task_type";
    public static final String URL_TASK_TYPE_DEFAULT = "SparkSQLTask";

    public static final String URL_DATA_SOURCE = "datasource_connection_name";
    public static final String URL_DATA_SOURCE_DEFAULT = "DataLakeCatalog";

    public static final String AUTH_SECRET_ID = "secret_id";
    public static final String AUTH_SECRET_KEY = "secret_key";

    public static final String REWRITE_DB_NAME = "db_name";
    public static final String REWRITE_TABLE_NAME = "table_name";

    public SyncRewriteDataFilesActionOption(Map<String, String> tableProperties) {
        this.properties = new HashMap<>();
        properties.put(URL_REGION, tableProperties.get(Constants.DLC_REGION_CONF));
        properties.put(AUTH_SECRET_ID, tableProperties.get(Constants.DLC_SECRET_ID_CONF));
        properties.put(AUTH_SECRET_KEY, tableProperties.get(Constants.DLC_SECRET_KEY_CONF));
        properties.put(URL_DEFAULT_DATABASE, tableProperties.get(FlinkCatalogFactory.DEFAULT_DATABASE));
        properties.put(REWRITE_DB_NAME, tableProperties.get(CATALOG_DATABASE.key()));
        properties.put(REWRITE_TABLE_NAME, tableProperties.get(CATALOG_TABLE.key()));
    }

    protected Map<String, String> getProperties() {
        return properties;
    }
}
