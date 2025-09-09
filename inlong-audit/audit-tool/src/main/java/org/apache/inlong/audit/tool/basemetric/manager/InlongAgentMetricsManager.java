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
public class InlongAgentMetricsManager {

    private Integer periodSeconds;

    private final CollectorRegistry collectorRegistry;

    private Gauge agentReceiveSuccessNumGauge;
    private Gauge agentReceiveSuccessSizeGauge;
    private Gauge agentReceiveSuccessAvgDelayGauge;
    private Gauge agentSendSuccessNumGauge;
    private Gauge agentSendSuccessSizeGauge;
    private Gauge agentSendSuccessAvgDelayGauge;
    private Gauge agentAbandonRateGauge;
    private Gauge agentLossRateGauge;

    public InlongAgentMetricsManager(CollectorRegistry collectorRegistry, Integer periodSeconds) {
        this.periodSeconds = periodSeconds;
        this.collectorRegistry = collectorRegistry;
        initMetrics();
    }

    private void initMetrics() {
        agentReceiveSuccessNumGauge = Gauge.build().name("inlong_agent_receive_num_interval").help("Number of messages received successfully by Inlong Agent in the last " + periodSeconds + " seconds").register(collectorRegistry);

        agentReceiveSuccessSizeGauge = Gauge.build().name("inlong_agent_receive_size_interval").help("Size of messages received successfully by Inlong Agent in the last " + periodSeconds + " seconds").register(collectorRegistry);

        agentReceiveSuccessAvgDelayGauge = Gauge.build().name("inlong_agent_receive_avg_delay_interval").help("Average delay of messages received successfully by Inlong Agent in the last " + periodSeconds + " seconds").register(collectorRegistry);

        agentSendSuccessNumGauge = Gauge.build().name("inlong_agent_send_num_interval").help("Number of messages sent successfully by Inlong Agent in the last " + periodSeconds + " seconds").register(collectorRegistry);

        agentSendSuccessSizeGauge = Gauge.build().name("inlong_agent_send_size_interval").help("Size of messages sent successfully by Inlong Agent in the last " + periodSeconds + " seconds").register(collectorRegistry);

        agentSendSuccessAvgDelayGauge = Gauge.build().name("inlong_agent_send_avg_delay_interval").help("Average delay of messages sent successfully by Inlong Agent in the last " + periodSeconds + " seconds").register(collectorRegistry);

        agentAbandonRateGauge = Gauge.build().name("inlong_agent_abandon_rate_interval").help("Abandon rate of messages by Inlong Agent in the last " + periodSeconds + " seconds").register(collectorRegistry);
        agentLossRateGauge = Gauge.build().name("inlong_agent_loss_rate_interval").help("Loss rate of messages by Inlong Agent in the last " + periodSeconds + " seconds").register(collectorRegistry);
    }
    public void updateReceiveSuccessNum(long num) {
        agentReceiveSuccessNumGauge.set(num);
    }

    public void updateReceiveSuccessSize(long size) {
        agentReceiveSuccessSizeGauge.set(size);
    }

    public void updateReceiveSuccessAvgDelay(long avgDelay) {
        agentReceiveSuccessAvgDelayGauge.set(avgDelay);
    }

    public void updateSendSuccessNum(long num) {
        agentSendSuccessNumGauge.set(num);
    }

    public void updateSendSuccessSize(long size) {
        agentSendSuccessSizeGauge.set(size);
    }

    public void updateSendSuccessAvgDelay(long avgDelay) {
        agentSendSuccessAvgDelayGauge.set(avgDelay);
    }

    public void updateAbandonRate(double rate) {
        agentAbandonRateGauge.set(rate);
    }

    public long getReceiveSuccessNum() {
        return (long) agentReceiveSuccessNumGauge.get();
    }

    public long getReceiveSuccessSize() {
        return (long) agentReceiveSuccessSizeGauge.get();
    }

    public long getReceiveSuccessAvgDelay() {
        return (long) agentReceiveSuccessAvgDelayGauge.get();
    }

    public long getSendSuccessNum() {
        return (long) agentSendSuccessNumGauge.get();
    }

    public long getSendSuccessSize() {
        return (long) agentSendSuccessSizeGauge.get();
    }

    public long getSendSuccessAvgDelay() {
        return (long) agentSendSuccessAvgDelayGauge.get();
    }

    public double getAbandonRate() {
        return agentAbandonRateGauge.get();
    }

    public void updateLossRate(double rate) {
        agentLossRateGauge.set(rate);
    }

    public double getLossRate() {
        return agentLossRateGauge.get();
    }
}