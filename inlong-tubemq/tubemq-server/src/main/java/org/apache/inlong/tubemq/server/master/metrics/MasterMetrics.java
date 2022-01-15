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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.inlong.tubemq.corebase.metric.AbsMetricItem;
import org.apache.inlong.tubemq.corebase.metric.CountMetricItem;
import org.apache.inlong.tubemq.corebase.metric.GaugeMaxMetricItem;
import org.apache.inlong.tubemq.corebase.metric.GaugeMinMetricItem;
import org.apache.inlong.tubemq.corebase.metric.GaugeNormMetricItem;
import org.apache.inlong.tubemq.corebase.metric.MetricValues;
import org.apache.inlong.tubemq.corebase.utils.DateTimeConvertUtils;

public class MasterMetrics implements MasterMetricMXBean {

    // statistics time since last reset
    private final AtomicLong lastResetTime =
            new AtomicLong(System.currentTimeMillis());
    // consume group statistics
    protected final AbsMetricItem consumeGroupCnt =
            new GaugeNormMetricItem("consume_group_cnt");
    protected final AbsMetricItem consumeGroupTmoTotCnt =
            new CountMetricItem("consume_group_timeout_cnt");
    protected final AbsMetricItem cltBalConsumeGroupCnt =
            new GaugeNormMetricItem("client_balance_group_cnt");
    protected final AbsMetricItem cltBalGroupTmototCnt =
            new CountMetricItem("clt_balance_timeout_cnt");
    // consumer client statistics
    protected final AbsMetricItem consumerOnlineCnt =
            new GaugeNormMetricItem("consumer_online_cnt");
    protected final AbsMetricItem consumerTmoTotCnt =
            new CountMetricItem("consumer_timeout_cnt");
    // producer client statistics
    protected final AbsMetricItem producerOnlineCnt =
            new GaugeNormMetricItem("producer_online_cnt");
    protected final AbsMetricItem producerTmoTotCnt =
            new CountMetricItem("producer_timeout_cnt");
    // broker node statistics
    protected final AbsMetricItem brokerConfigCnt =
            new GaugeNormMetricItem("broker_configure_cnt");
    protected final AbsMetricItem brokerOnlineCnt =
            new GaugeNormMetricItem("broker_online_cnt");
    protected final AbsMetricItem brokerTmoTotCnt =
            new CountMetricItem("broker_timeout_cnt");
    protected final AbsMetricItem brokerAbnCurCnt =
            new GaugeNormMetricItem("broker_abn_current_cnt");
    protected final AbsMetricItem brokerAbnTotCnt =
            new CountMetricItem("broker_abn_total_cnt");
    protected final AbsMetricItem brokerFbdCurCnt =
            new GaugeNormMetricItem("broker_fbd_current_cnt");
    protected final AbsMetricItem brokerFbdTotCnt =
            new CountMetricItem("broker_fbd_total_cnt");
    // server balance statistics
    protected final AbsMetricItem svrBalDuration =
            new GaugeNormMetricItem("svrbalance_duration");
    protected final AbsMetricItem svrBalDurationMin =
            new GaugeMinMetricItem("svrbalance_duration_min");
    protected final AbsMetricItem svrBalDurationMax =
            new GaugeMaxMetricItem("svrbalance_duration_max");
    protected final AbsMetricItem svrBalResetDurMin =
            new GaugeMinMetricItem("svrbal_reset_duration_min");
    protected final AbsMetricItem svrBalResetDurMax =
            new GaugeMaxMetricItem("svrbal_reset_duration_max");
    protected final AbsMetricItem svrBalConEventConsumerCnt =
            new GaugeNormMetricItem("svrbal_con_consumer_cnt");
    protected final AbsMetricItem svrBalDisConEventConsumerCnt =
            new GaugeNormMetricItem("svrbal_discon_consumer_cnt");

    @Override
    public MetricValues getMetrics() {
        Map<String, Long> metricValues = new HashMap<>();
        metricValues.put(consumeGroupCnt.getName(), consumeGroupCnt.getValue());
        metricValues.put(consumeGroupTmoTotCnt.getName(), consumeGroupTmoTotCnt.getValue());
        metricValues.put(cltBalConsumeGroupCnt.getName(), cltBalConsumeGroupCnt.getValue());
        metricValues.put(cltBalGroupTmototCnt.getName(), cltBalGroupTmototCnt.getValue());
        metricValues.put(consumerOnlineCnt.getName(), consumerOnlineCnt.getValue());
        metricValues.put(consumerTmoTotCnt.getName(), consumerTmoTotCnt.getValue());
        metricValues.put(producerOnlineCnt.getName(), producerOnlineCnt.getValue());
        metricValues.put(producerTmoTotCnt.getName(), producerTmoTotCnt.getValue());
        metricValues.put(brokerConfigCnt.getName(), brokerConfigCnt.getValue());
        metricValues.put(brokerOnlineCnt.getName(), brokerOnlineCnt.getValue());
        metricValues.put(brokerTmoTotCnt.getName(), brokerTmoTotCnt.getValue());
        metricValues.put(brokerAbnCurCnt.getName(), brokerAbnCurCnt.getValue());
        metricValues.put(brokerAbnTotCnt.getName(), brokerAbnTotCnt.getValue());
        metricValues.put(brokerFbdCurCnt.getName(), brokerFbdCurCnt.getValue());
        metricValues.put(brokerFbdTotCnt.getName(), brokerFbdTotCnt.getValue());
        metricValues.put(svrBalDuration.getName(), svrBalDuration.getValue());
        metricValues.put(svrBalDurationMin.getName(), svrBalDurationMin.getValue());
        metricValues.put(svrBalDurationMax.getName(), svrBalDurationMax.getValue());
        metricValues.put(svrBalResetDurMin.getName(), svrBalResetDurMin.getValue());
        metricValues.put(svrBalResetDurMax.getName(), svrBalResetDurMax.getValue());
        metricValues.put(svrBalConEventConsumerCnt.getName(),
                svrBalConEventConsumerCnt.getValue());
        metricValues.put(svrBalDisConEventConsumerCnt.getName(),
                svrBalDisConEventConsumerCnt.getValue());
        return new MetricValues(
                DateTimeConvertUtils.ms2yyyyMMddHHmmss(lastResetTime.get()), metricValues);
    }

    @Override
    public MetricValues getAndReSetMetrics() {
        Map<String, Long> metricValues = new HashMap<>();
        metricValues.put(consumeGroupCnt.getName(), consumeGroupCnt.getAndSet());
        metricValues.put(consumeGroupTmoTotCnt.getName(), consumeGroupTmoTotCnt.getAndSet());
        metricValues.put(cltBalConsumeGroupCnt.getName(), cltBalConsumeGroupCnt.getAndSet());
        metricValues.put(cltBalGroupTmototCnt.getName(), cltBalGroupTmototCnt.getAndSet());
        metricValues.put(consumerOnlineCnt.getName(), consumerOnlineCnt.getAndSet());
        metricValues.put(consumerTmoTotCnt.getName(), consumerTmoTotCnt.getAndSet());
        metricValues.put(producerOnlineCnt.getName(), producerOnlineCnt.getAndSet());
        metricValues.put(producerTmoTotCnt.getName(), producerTmoTotCnt.getAndSet());
        metricValues.put(brokerConfigCnt.getName(), brokerConfigCnt.getAndSet());
        metricValues.put(brokerOnlineCnt.getName(), brokerOnlineCnt.getAndSet());
        metricValues.put(brokerTmoTotCnt.getName(), brokerTmoTotCnt.getAndSet());
        metricValues.put(brokerAbnCurCnt.getName(), brokerAbnCurCnt.getAndSet());
        metricValues.put(brokerAbnTotCnt.getName(), brokerAbnTotCnt.getAndSet());
        metricValues.put(brokerFbdCurCnt.getName(), brokerFbdCurCnt.getAndSet());
        metricValues.put(brokerFbdTotCnt.getName(), brokerFbdTotCnt.getAndSet());
        metricValues.put(svrBalDuration.getName(), svrBalDuration.getAndSet());
        metricValues.put(svrBalDurationMin.getName(), svrBalDurationMin.getAndSet());
        metricValues.put(svrBalDurationMax.getName(), svrBalDurationMax.getAndSet());
        metricValues.put(svrBalResetDurMin.getName(), svrBalResetDurMin.getAndSet());
        metricValues.put(svrBalResetDurMax.getName(), svrBalResetDurMax.getAndSet());
        metricValues.put(svrBalConEventConsumerCnt.getName(),
                svrBalConEventConsumerCnt.getAndSet());
        metricValues.put(svrBalDisConEventConsumerCnt.getName(),
                svrBalDisConEventConsumerCnt.getAndSet());
        alignBrokerFbdMetrics();
        alignBrokerAbnMetrics();
        long befTime = lastResetTime.getAndSet(System.currentTimeMillis());
        return new MetricValues(
                DateTimeConvertUtils.ms2yyyyMMddHHmmss(befTime), metricValues);
    }

    public long getLastResetTime() {
        return lastResetTime.get();
    }

    public AbsMetricItem getConsumeGroupCnt() {
        return consumeGroupCnt;
    }

    public AbsMetricItem getConsumeGroupTmoTotCnt() {
        return consumeGroupTmoTotCnt;
    }

    public AbsMetricItem getCltBalConsumeGroupCnt() {
        return cltBalConsumeGroupCnt;
    }

    public AbsMetricItem getCltBalGroupTmototCnt() {
        return cltBalGroupTmototCnt;
    }

    public AbsMetricItem getConsumerOnlineCnt() {
        return consumerOnlineCnt;
    }

    public AbsMetricItem getConsumerTmoTotCnt() {
        return consumerTmoTotCnt;
    }

    public AbsMetricItem getProducerOnlineCnt() {
        return producerOnlineCnt;
    }

    public AbsMetricItem getProducerTmoTotCnt() {
        return producerTmoTotCnt;
    }

    public AbsMetricItem getBrokerConfigCnt() {
        return brokerConfigCnt;
    }

    public AbsMetricItem getBrokerOnlineCnt() {
        return brokerOnlineCnt;
    }

    public AbsMetricItem getBrokerTmoTotCnt() {
        return brokerTmoTotCnt;
    }

    public AbsMetricItem getBrokerAbnCurCnt() {
        return brokerAbnCurCnt;
    }

    public AbsMetricItem getBrokerAbnTotCnt() {
        return brokerAbnTotCnt;
    }

    public AbsMetricItem getBrokerFbdCurCnt() {
        return brokerFbdCurCnt;
    }

    public AbsMetricItem getBrokerFbdTotCnt() {
        return brokerFbdTotCnt;
    }

    public AbsMetricItem getSvrBalDuration() {
        return svrBalDuration;
    }

    public AbsMetricItem getSvrBalDurationMin() {
        return svrBalDurationMin;
    }

    public AbsMetricItem getSvrBalDurationMax() {
        return svrBalDurationMax;
    }

    public AbsMetricItem getSvrBalResetDurMin() {
        return svrBalResetDurMin;
    }

    public AbsMetricItem getSvrBalResetDurMax() {
        return svrBalResetDurMax;
    }

    public AbsMetricItem getSvrBalConEventConsumerCnt() {
        return svrBalConEventConsumerCnt;
    }

    public AbsMetricItem getSvrBalDisConEventConsumerCnt() {
        return svrBalDisConEventConsumerCnt;
    }

    private void alignBrokerFbdMetrics() {
        // Notice: the minimum value of the brokerFbdTotCnt metric value is
        //         the current value of brokerFbdCurCnt, so the metric value
        //         needs to be aligned after reset
        long curCnt = brokerFbdCurCnt.getValue();
        long totalCnt = brokerFbdTotCnt.getValue();
        while (curCnt > totalCnt) {
            if (brokerFbdTotCnt.compareAndSet(totalCnt, curCnt)) {
                break;
            }
            curCnt = brokerFbdCurCnt.getValue();
            totalCnt = brokerFbdTotCnt.getValue();
        }
    }

    private void alignBrokerAbnMetrics() {
        // Notice: the minimum value of the brokerAbnTotCnt metric value is
        //         the current value of brokerAbnCurCnt, so the metric value
        //         needs to be aligned after reset
        long curCnt = brokerAbnCurCnt.getValue();
        long totalCnt = brokerAbnTotCnt.getValue();
        while (curCnt > totalCnt) {
            if (brokerAbnTotCnt.compareAndSet(totalCnt, curCnt)) {
                break;
            }
            curCnt = brokerAbnCurCnt.getValue();
            totalCnt = brokerAbnTotCnt.getValue();
        }
    }
}

