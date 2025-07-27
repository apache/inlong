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

package org.apache.inlong.sort.clickhouse.source;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Converts each row into JSON string.
 */
public class ClickHouseRowConverter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String convert(ResultSet rs) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        Map<String, Object> map = new java.util.HashMap<>(cols);
        for (int i = 1; i <= cols; i++) {
            map.put(meta.getColumnName(i), rs.getObject(i));
        }
        return MAPPER.writeValueAsString(map);
    }
}
