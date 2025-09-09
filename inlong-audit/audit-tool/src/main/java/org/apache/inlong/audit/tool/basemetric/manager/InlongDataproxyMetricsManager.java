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

import io.prometheus.client.Gauge;
import io.prometheus.client.CollectorRegistry;
import org.springframework.stereotype.Component;

@Component
public class InlongDataproxyMetricsManager {

    private Integer periodSeconds;

    private final CollectorRegistry collectorRegistry;

    private Gauge dataproxyReceiveSuccessNumGauge;
    private Gauge dataproxyReceiveSuccessSizeGauge;
    private Gauge dataproxyReceiveSuccessAvgDelayGauge;
    private Gauge dataproxySendSuccessNumGauge;
    private Gauge dataproxySendSuccessSizeGauge;
    private Gauge dataproxySendSuccessAvgDelayGauge;
    private Gauge dataproxyAbandonRateGauge;
    private Gauge dataproxyLossRateGauge;

    public InlongDataproxyMetricsManager(CollectorRegistry collectorRegistry,Integer periodSeconds) {
        this.collectorRegistry = collectorRegistry;
        this.periodSeconds=periodSeconds;
        initMetrics();
    }

    private void initMetrics() {
        dataproxyReceiveSuccessNumGauge = Gauge.build()
                .name("inlong_dataproxy_receive_num_interval")
                .help("Number of messages received successfully by Inlong Dataproxy in the last " + periodSeconds + " seconds")
                .register(collectorRegistry);

        dataproxyReceiveSuccessSizeGauge = Gauge.build()
                .name("inlong_dataproxy_receive_size_interval")
                .help("Size of messages received successfully by Inlong Dataproxy in the last " + periodSeconds + " seconds")
                .register(collectorRegistry);

        dataproxyReceiveSuccessAvgDelayGauge = Gauge.build()
                .name("inlong_dataproxy_receive_avg_delay_interval")
                .help("Average delay of messages received successfully by Inlong Dataproxy in the last " + periodSeconds + " seconds")
                .register(collectorRegistry);

        dataproxySendSuccessNumGauge = Gauge.build()
                .name("inlong_dataproxy_send_num_interval")
                .help("Number of messages sent successfully by Inlong Dataproxy in the last " + periodSeconds+ " seconds")
                .register(collectorRegistry);

        dataproxySendSuccessSizeGauge = Gauge.build()
                .name("inlong_dataproxy_send_size_interval")
                .help("Size of messages sent successfully by Inlong Dataproxy in the last " + periodSeconds+ " seconds")
                .register(collectorRegistry);

        dataproxySendSuccessAvgDelayGauge = Gauge.build()
                .name("inlong_dataproxy_send_avg_delay_interval")
                .help("Average delay of messages sent successfully by Inlong Dataproxy in the last " + periodSeconds+ " seconds")
                .register(collectorRegistry);

        dataproxyAbandonRateGauge = Gauge.build()
                .name("inlong_dataproxy_abandon_rate_interval")
                .help("Abandon rate of messages by Inlong Dataproxy in the last " + periodSeconds+ " seconds")
                .register(collectorRegistry);

        dataproxyLossRateGauge = Gauge.build()
                .name("inlong_dataproxy_loss_rate_interval")
                .help("Loss rate of messages by Inlong Dataproxy in the last " + periodSeconds + " seconds")
                .register(collectorRegistry);
    }

    public void updateReceiveSuccessNum(long num) {
        dataproxyReceiveSuccessNumGauge.set(num);
    }

    public void updateReceiveSuccessSize(long size) {
        dataproxyReceiveSuccessSizeGauge.set(size);
    }

    public void updateReceiveSuccessAvgDelay(long avgDelay) {
        dataproxyReceiveSuccessAvgDelayGauge.set(avgDelay);
    }

    public void updateSendSuccessNum(long num) {
        dataproxySendSuccessNumGauge.set(num);
    }

    public void updateSendSuccessSize(long size) {
        dataproxySendSuccessSizeGauge.set(size);
    }

    public void updateSendSuccessAvgDelay(long avgDelay) {
        dataproxySendSuccessAvgDelayGauge.set(avgDelay);
    }

    public void updateAbandonRate(double rate) {
        dataproxyAbandonRateGauge.set(rate);
    }

    public void updateLossRate(double rate) {
        dataproxyLossRateGauge.set(rate);
    }

    public long getReceiveSuccessNum() {
        return (long) dataproxyReceiveSuccessNumGauge.get();
    }

    public long getReceiveSuccessSize() {
        return (long) dataproxyReceiveSuccessSizeGauge.get();
    }

    public long getReceiveSuccessAvgDelay() {
        return (long) dataproxyReceiveSuccessAvgDelayGauge.get();
    }

    public long getSendSuccessNum() {
        return (long) dataproxySendSuccessNumGauge.get();
    }

    public long getSendSuccessSize() {
        return (long) dataproxySendSuccessSizeGauge.get();
    }

    public long getSendSuccessAvgDelay() {
        return (long) dataproxySendSuccessAvgDelayGauge.get();
    }

    public double getAbandonRate() {
        return dataproxyAbandonRateGauge.get();
    }

    public double getLossRate() {
        return dataproxyLossRateGauge.get();
    }
}