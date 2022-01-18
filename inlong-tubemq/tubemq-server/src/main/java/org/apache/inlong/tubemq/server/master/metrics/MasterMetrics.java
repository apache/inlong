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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.inlong.tubemq.corebase.metric.AbsMetricItem;
import org.apache.inlong.tubemq.corebase.metric.CountMetricItem;
import org.apache.inlong.tubemq.corebase.metric.GaugeNormMetricItem;
import org.apache.inlong.tubemq.corebase.metric.MetricValues;
import org.apache.inlong.tubemq.corebase.metric.TimeDltMetricItem;
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
    protected TimeDltMetricItem svrBalDltItem =
            new TimeDltMetricItem("svr_balance");
    protected TimeDltMetricItem svrBalResetDltItem =
            new TimeDltMetricItem("svrbal_reset");
    protected final AbsMetricItem svrBalConEventConsumerCnt =
            new GaugeNormMetricItem("svrbal_con_consumer_cnt");
    protected final AbsMetricItem svrBalDisConEventConsumerCnt =
            new GaugeNormMetricItem("svrbal_discon_consumer_cnt");

    @Override
    public MetricValues getMetrics() {
        return snapshotMetrics(false);
    }

    @Override
    public MetricValues getAndReSetMetrics() {
        return snapshotMetrics(true);
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

    public AbsMetricItem getSvrBalConEventConsumerCnt() {
        return svrBalConEventConsumerCnt;
    }

    public AbsMetricItem getSvrBalDisConEventConsumerCnt() {
        return svrBalDisConEventConsumerCnt;
    }

    public TimeDltMetricItem getSvrBalDltItem() {
        return svrBalDltItem;
    }

    public TimeDltMetricItem getSvrBalResetDltItem() {
        return svrBalResetDltItem;
    }

    private void alignBrokerFbdMetrics() {
        // Notice: the minimum value of the brokerFbdTotCnt metric value is
        //         the current value of brokerFbdCurCnt, so the metric value
        //         needs to be aligned after reset
        long curCnt = brokerFbdCurCnt.getValue(false);
        long totalCnt = brokerFbdTotCnt.getValue(false);
        while (curCnt > totalCnt) {
            if (brokerFbdTotCnt.compareAndSet(totalCnt, curCnt)) {
                break;
            }
            curCnt = brokerFbdCurCnt.getValue(false);
            totalCnt = brokerFbdTotCnt.getValue(false);
        }
    }

    private void alignBrokerAbnMetrics() {
        // Notice: the minimum value of the brokerAbnTotCnt metric value is
        //         the current value of brokerAbnCurCnt, so the metric value
        //         needs to be aligned after reset
        long curCnt = brokerAbnCurCnt.getValue(false);
        long totalCnt = brokerAbnTotCnt.getValue(false);
        while (curCnt > totalCnt) {
            if (brokerAbnTotCnt.compareAndSet(totalCnt, curCnt)) {
                break;
            }
            curCnt = brokerAbnCurCnt.getValue(false);
            totalCnt = brokerAbnTotCnt.getValue(false);
        }
    }

    private MetricValues snapshotMetrics(boolean resetValue) {
        Map<String, Long> metricValues = new LinkedHashMap<>();
        metricValues.put(consumeGroupCnt.getName(), consumeGroupCnt.getValue(resetValue));
        metricValues.put(consumeGroupTmoTotCnt.getName(), consumeGroupTmoTotCnt.getValue(resetValue));
        metricValues.put(cltBalConsumeGroupCnt.getName(), cltBalConsumeGroupCnt.getValue(resetValue));
        metricValues.put(cltBalGroupTmototCnt.getName(), cltBalGroupTmototCnt.getValue(resetValue));
        metricValues.put(consumerOnlineCnt.getName(), consumerOnlineCnt.getValue(resetValue));
        metricValues.put(consumerTmoTotCnt.getName(), consumerTmoTotCnt.getValue(resetValue));
        metricValues.put(producerOnlineCnt.getName(), producerOnlineCnt.getValue(resetValue));
        metricValues.put(producerTmoTotCnt.getName(), producerTmoTotCnt.getValue(resetValue));
        metricValues.put(brokerConfigCnt.getName(), brokerConfigCnt.getValue(resetValue));
        metricValues.put(brokerOnlineCnt.getName(), brokerOnlineCnt.getValue(resetValue));
        metricValues.put(brokerTmoTotCnt.getName(), brokerTmoTotCnt.getValue(resetValue));
        metricValues.put(brokerAbnCurCnt.getName(), brokerAbnCurCnt.getValue(resetValue));
        metricValues.put(brokerAbnTotCnt.getName(), brokerAbnTotCnt.getValue(resetValue));
        metricValues.put(brokerFbdCurCnt.getName(), brokerFbdCurCnt.getValue(resetValue));
        metricValues.put(brokerFbdTotCnt.getName(), brokerFbdTotCnt.getValue(resetValue));
        svrBalDltItem.getProcTimeDltDuration(metricValues, resetValue);
        svrBalResetDltItem.getProcTimeDltDuration(metricValues, resetValue);
        metricValues.put(svrBalConEventConsumerCnt.getName(),
                svrBalConEventConsumerCnt.getValue(resetValue));
        metricValues.put(svrBalDisConEventConsumerCnt.getName(),
                svrBalDisConEventConsumerCnt.getValue(resetValue));
        if (resetValue) {
            alignBrokerFbdMetrics();
            alignBrokerAbnMetrics();
            long befTime = lastResetTime.getAndSet(System.currentTimeMillis());
            return new MetricValues(
                    DateTimeConvertUtils.ms2yyyyMMddHHmmss(befTime), metricValues);
        } else {
            return new MetricValues(
                    DateTimeConvertUtils.ms2yyyyMMddHHmmss(lastResetTime.get()), metricValues);
        }
    }

}

