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

package org.apache.inlong.sort.redis;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.inlong.sort.redis.sink.RedisDynamicTableSink;
import org.junit.Test;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.junit.Assert.assertTrue;

public class RedisDynamicTableFactoryTest {

    private static final ResolvedSchema TEST_SCHEMA = ResolvedSchema.of(
            Column.physical("key", DataTypes.STRING()),
            Column.physical("f1", DataTypes.BIGINT()),
            Column.physical("f2", DataTypes.INT()),
            Column.physical("f3", DataTypes.FLOAT()),
            Column.physical("f4", DataTypes.DOUBLE()),
            Column.physical("f5", DataTypes.BOOLEAN()));

    private Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>();

        properties.put("connector", "redis-inlong");

        properties.put("schema.0.name", "key");
        properties.put("schema.0.type", "STRING");
        properties.put("schema.1.name", "f1");
        properties.put("schema.1.type", "BIGINT");
        properties.put("schema.2.name", "f2");
        properties.put("schema.2.type", "INT");
        properties.put("schema.3.name", "f3");
        properties.put("schema.3.type", "FLOAT");
        properties.put("schema.4.name", "f4");
        properties.put("schema.4.type", "DOUBLE");
        properties.put("schema.5.name", "f5");
        properties.put("schema.5.type", "BOOLEAN");

        properties.put("format.type", "csv");
        properties.put("format.property-version", "1");
        properties.put("format.derive-schema", "true");

        return properties;
    }

    @Test
    public void testCreateTableSink() {
        Map<String, String> properties = getProperties();

        DynamicTableSink tableSink = createTableSink(TEST_SCHEMA, properties);
        assertTrue(tableSink instanceof RedisDynamicTableSink);
    }
}
