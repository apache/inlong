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

import org.apache.inlong.sort.clickhouse.protocol.SourceAudit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

/**
 * Reads from ClickHouse using JDBC, supports incremental fetch and audit.
 */
public class ClickHouseSource extends RichSourceFunction<String> implements SourceFunction<String> {

    private final ClickHouseSourceConfig config;
    private volatile boolean running = true;
    private Object lastOffset = 0;

    public ClickHouseSource(ClickHouseSourceConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(config.getDriverClassName());
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (running) {
            String sql = config.buildQuery(lastOffset);
            try (Connection conn = DriverManager.getConnection(config.getJdbcUrl(),
                    createProps());
                    PreparedStatement stmt = conn.prepareStatement(sql)) {

                stmt.setFetchSize(config.getFetchSize());
                try (ResultSet rs = stmt.executeQuery()) {
                    while (running && rs.next()) {
                        String record = ClickHouseRowConverter.convert(rs);
                        if (config.getInlongAudit() != null) {
                            SourceAudit.audit(record, config.getInlongAudit());
                        }
                        ctx.collect(record);
                        lastOffset = rs.getObject(config.getChunkKeyColumn());
                    }
                }
            }
            // small sleep to avoid busy-loop
            Thread.sleep(1000);
        }
    }

    private Properties createProps() {
        Properties props = new Properties();
        props.put("user", config.getUsername());
        props.put("password", config.getPassword());
        return props;
    }

    @Override
    public void cancel() {
        running = false;
    }
}
