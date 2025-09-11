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
import org.springframework.stereotype.Component;

@Component
public class InlongApiMetricsManager {

    private Integer periodSeconds;

    private final CollectorRegistry collectorRegistry;

    private Gauge apiReceiveSuccessNumGauge;
    private Gauge apiReceiveSuccessSizeGauge;
    private Gauge apiReceiveSuccessAvgDelayGauge;
    private Gauge apiSendSuccessNumGauge;
    private Gauge apiSendSuccessSizeGauge;
    private Gauge apiSendSuccessAvgDelayGauge;
    private Gauge apiAbandonRateGauge;

    public InlongApiMetricsManager(CollectorRegistry collectorRegistry, Integer periodSeconds) {
        this.collectorRegistry = collectorRegistry;
        this.periodSeconds = periodSeconds;
        initMetrics();
    }

    private void initMetrics() {
        apiReceiveSuccessNumGauge = Gauge.build()
                .name("inlong_api_receive_num_interval")
                .help("Number of messages received successfully by Inlong API in the last " + periodSeconds + " seconds")
                .register(collectorRegistry);

        apiReceiveSuccessSizeGauge = Gauge.build()
                .name("inlong_api_receive_size_interval")
                .help("Size of messages received successfully by Inlong API in the last " + periodSeconds + " seconds")
                .register(collectorRegistry);

        apiReceiveSuccessAvgDelayGauge = Gauge.build()
                .name("inlong_api_receive_avg_delay_interval")
                .help("Average delay of messages received successfully by Inlong API in the last " + periodSeconds + " seconds")
                .register(collectorRegistry);

        apiSendSuccessNumGauge = Gauge.build()
                .name("inlong_api_send_num_interval")
                .help("Number of messages sent successfully by Inlong API in the last " + periodSeconds + " seconds")
                .register(collectorRegistry);

        apiSendSuccessSizeGauge = Gauge.build()
                .name("inlong_api_send_size_interval")
                .help("Size of messages sent successfully by Inlong API in the last " + periodSeconds + " seconds")
                .register(collectorRegistry);

        apiSendSuccessAvgDelayGauge = Gauge.build()
                .name("inlong_api_send_avg_delay_interval")
                .help("Average delay of messages sent successfully by Inlong API in the last " + periodSeconds + " seconds")
                .register(collectorRegistry);

        apiAbandonRateGauge = Gauge.build()
                .name("inlong_api_abandon_rate_interval")
                .help("Abandon rate of messages by Inlong API in the last " + periodSeconds + " seconds")
                .register(collectorRegistry);
    }

    public void updateReceiveSuccessNum(long num) {
        apiReceiveSuccessNumGauge.set(num);
    }

    public void updateReceiveSuccessSize(long size) {
        apiReceiveSuccessSizeGauge.set(size);
    }

    public void updateReceiveSuccessAvgDelay(long avgDelay) {
        apiReceiveSuccessAvgDelayGauge.set(avgDelay);
    }

    public void updateSendSuccessNum(long num) {
        apiSendSuccessNumGauge.set(num);
    }

    public void updateSendSuccessSize(long size) {
        apiSendSuccessSizeGauge.set(size);
    }

    public void updateSendSuccessAvgDelay(long avgDelay) {
        apiSendSuccessAvgDelayGauge.set(avgDelay);
    }

    public void updateAbandonRate(double rate) {
        apiAbandonRateGauge.set(rate);
    }

    public long getReceiveSuccessNum() {
        return (long) apiReceiveSuccessNumGauge.get();
    }

    public long getReceiveSuccessSize() {
        return (long) apiReceiveSuccessSizeGauge.get();
    }

    public long getReceiveSuccessAvgDelay() {
        return (long) apiReceiveSuccessAvgDelayGauge.get();
    }

    public long getSendSuccessNum() {
        return (long) apiSendSuccessNumGauge.get();
    }

    public long getSendSuccessSize() {
        return (long) apiSendSuccessSizeGauge.get();
    }

    public long getSendSuccessAvgDelay() {
        return (long) apiSendSuccessAvgDelayGauge.get();
    }

    public double getAbandonRate() {
        return apiAbandonRateGauge.get();
    }
}