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

package org.apache.inlong.audit.tool.basemetric.manager;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;

public class InlongAuditMetricsManager {

    private final Gauge messageNumGauge;
    private final Gauge messageSizeGauge;
    private final Gauge messageAvgDelayGauge;

    public InlongAuditMetricsManager(CollectorRegistry registry) {
        this.messageNumGauge = Gauge.build()
                .name("inlong_audit_log_message_num_interval")
                .help("Number of audit logs recorded by Inlong in the last interval")
                .register(registry);

        this.messageSizeGauge = Gauge.build()
                .name("inlong_audit_log_message_size_interval")
                .help("Size of audit logs recorded by Inlong in the last interval")
                .register(registry);

        this.messageAvgDelayGauge = Gauge.build()
                .name("inlong_audit_log_message_avg_delay_interval")
                .help("Average delay of audit logs recorded by Inlong in the last interval")
                .register(registry);
    }

    public void reportBasicMetrics(long messageNum, long messageSize, long avgDelay) {
        messageNumGauge.set(messageNum);
        messageSizeGauge.set(messageSize);
        messageAvgDelayGauge.set(avgDelay);
    }

    public void updateMessageNum(long num) {
        messageNumGauge.set(num);
    }

    public void updateMessageSize(long size) {
        messageSizeGauge.set(size);
    }

    public void updateMessageAvgDelay(long avgDelay) {
        messageAvgDelayGauge.set(avgDelay);
    }
}