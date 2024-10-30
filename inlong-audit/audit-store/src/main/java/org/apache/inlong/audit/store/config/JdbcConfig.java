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

package org.apache.inlong.audit.store.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
public class JdbcConfig {

    @Value("${audit.store.jdbc.driver:com.mysql.cj.jdbc.Driver}")
    private String driver;
    @Value("${audit.store.jdbc.url}")
    private String url;
    @Value("${audit.store.jdbc.username}")
    private String userName;
    @Value("${audit.store.jdbc.password}")
    private String password;
    @Value("${audit.store.jdbc.batchIntervalMs:1000}")
    private int batchIntervalMs;
    @Value("${audit.store.jdbc.batchThreshold:500}")
    private int batchThreshold;
    @Value("${audit.store.jdbc.processIntervalMs:100}")
    private int processIntervalMs;
    @Value("${audit.store.data.queue.size:1000000}")
    private int dataQueueSize;
}
