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

package org.apache.inlong.tubemq.server.master.metrics;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.inlong.commons.config.metrics.CountMetric;
import org.apache.inlong.commons.config.metrics.Dimension;
import org.apache.inlong.commons.config.metrics.GaugeMetric;
import org.apache.inlong.commons.config.metrics.MetricDomain;
import org.apache.inlong.commons.config.metrics.MetricItem;
import org.apache.inlong.commons.config.metrics.MetricRegister;

@MetricDomain(name = "master_metrics")
public class MasterMetric extends MetricItem {

    private static final MasterMetric MASTER_METRICS = new MasterMetric();
    private static final AtomicBoolean REGISTER_ONCE =
            new AtomicBoolean(false);
    private static final String METRIC_NAME = "master_metrics";

    @Dimension
    public String tagName;

    @GaugeMetric
    public AtomicLong consumeGroupCnt = new AtomicLong(0);

    @GaugeMetric
    public AtomicLong cltBalConsumeGroupCnt = new AtomicLong(0);

    @CountMetric
    public AtomicLong consumeGroupTmoTotCnt = new AtomicLong(0);

    @GaugeMetric
    public AtomicLong consumerCnt = new AtomicLong(0);

    @CountMetric
    public AtomicLong consumerTmoTotCnt = new AtomicLong(0);

    @GaugeMetric
    public AtomicLong producerCnt = new AtomicLong(0);

    @CountMetric
    public AtomicLong producerTmoTotCnt = new AtomicLong(0);

    @GaugeMetric
    public AtomicLong brokerConfigCnt = new AtomicLong(0);

    @GaugeMetric
    public AtomicLong brokerOnlineCnt = new AtomicLong(0);

    @CountMetric
    public AtomicLong brokerAbnTotCnt = new AtomicLong(0);

    @GaugeMetric
    public AtomicLong brokerAbnCurCnt = new AtomicLong(0);

    @CountMetric
    public AtomicLong brokerFbdTotCnt = new AtomicLong(0);

    @GaugeMetric
    public AtomicLong brokerFbdCurCnt = new AtomicLong(0);

    @CountMetric
    public AtomicLong brokerTmoTotCnt = new AtomicLong(0);

    @GaugeMetric
    public AtomicLong svrBalLatency = new AtomicLong(0);

    @GaugeMetric
    public AtomicLong svrBalLatencyMax = new AtomicLong(0);

    @GaugeMetric
    public AtomicLong svrBalResetDurMin = new AtomicLong(Long.MAX_VALUE);

    @GaugeMetric
    public AtomicLong svrBalResetDurMax = new AtomicLong(0);

    @GaugeMetric
    public AtomicLong svrBalConEventConsumerCnt = new AtomicLong(0);

    @GaugeMetric
    public AtomicLong svrBalDisConEventConsumerCnt = new AtomicLong(0);

    public static MasterMetric create() {
        if (REGISTER_ONCE.compareAndSet(false, true)) {
            MASTER_METRICS.tagName = METRIC_NAME;
            MetricRegister.register(MASTER_METRICS);
        }
        return MASTER_METRICS;
    }
}

