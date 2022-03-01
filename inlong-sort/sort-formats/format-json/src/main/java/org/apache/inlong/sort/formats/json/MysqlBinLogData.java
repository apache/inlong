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

package org.apache.inlong.sort.formats.json;

import java.io.Serializable;
import java.util.Map;
import org.apache.flink.table.data.RowData;

public class MysqlBinLogData implements Serializable {

    public static final String MYSQL_METADATA_DATABASE = "mysql_metadata_database";

    public static final String MYSQL_METADATA_TABLE = "mysql_metadata_table";

    public static final String MYSQL_METADATA_EVENT_TIME = "mysql_metadata_event_time";

    public static final String MYSQL_METADATA_IS_DDL = "mysql_metadata_is_ddl";

    public static final String MYSQL_METADATA_EVENT_TYPE = "mysql_metadata_event_type";

    private RowData physicalData;

    private Map<String, Object> metadataMap;

    public MysqlBinLogData(RowData physicalData, Map<String, Object> metadataMap) {
        this.physicalData = physicalData;
        this.metadataMap = metadataMap;
    }

    public RowData getPhysicalData() {
        return physicalData;
    }

    public void setPhysicalData(RowData physicalData) {
        this.physicalData = physicalData;
    }

    public Map<String, Object> getMetadataMap() {
        return metadataMap;
    }

    public void setMetadataMap(Map<String, Object> metadataMap) {
        this.metadataMap = metadataMap;
    }
}
