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

package org.apache.inlong.tubemq.server.broker.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.inlong.tubemq.corebase.metric.AbsMetricItem;
import org.apache.inlong.tubemq.corebase.metric.CountMetricItem;
import org.apache.inlong.tubemq.corebase.metric.GaugeNormMetricItem;
import org.apache.inlong.tubemq.corebase.metric.MetricValues;
import org.apache.inlong.tubemq.corebase.metric.TimeDltMetricItem;
import org.apache.inlong.tubemq.corebase.utils.DateTimeConvertUtils;

public class BrokerMetrics implements BrokerMetricMXBean {

    private final AtomicLong lastResetTime =
            new AtomicLong(System.currentTimeMillis());
    // Delay statistics for syncing data to files
    protected final TimeDltMetricItem fileSyncTimeDltItem =
            new TimeDltMetricItem("file_sync");
    // Delay statistics for syncing data to Zookeeper
    protected final TimeDltMetricItem zkSyncTimeDltItem =
            new TimeDltMetricItem("zk_sync");
    // Zookeeper Exception statistics
    protected final AbsMetricItem zkExceptionCnt =
            new CountMetricItem("zk_exception_cnt");
    // Broker 2 Master status statistics
    protected final AbsMetricItem masterNoNodeCnt =
            new CountMetricItem("online_timeout_cnt");
    protected final AbsMetricItem hbExceptionCnt =
            new CountMetricItem("hb_master_exception_cnt");
    // Disk IO Exception statistics
    protected final AbsMetricItem ioExceptionCnt =
            new CountMetricItem("io_exception_cnt");
    // Consumer client statistics
    protected final AbsMetricItem consumerOnlineCnt =
            new GaugeNormMetricItem("consumer_online_cnt");
    protected final AbsMetricItem consumerTmoTotCnt =
            new CountMetricItem("consumer_timeout_cnt");

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

    public AbsMetricItem getZkExceptionCnt() {
        return zkExceptionCnt;
    }

    public AbsMetricItem getMasterNoNodeCnt() {
        return masterNoNodeCnt;
    }

    public AbsMetricItem getHbExceptionCnt() {
        return hbExceptionCnt;
    }

    public AbsMetricItem getIoExceptionCnt() {
        return ioExceptionCnt;
    }

    public AbsMetricItem getConsumerOnlineCnt() {
        return consumerOnlineCnt;
    }

    public AbsMetricItem getConsumerTmoTotCnt() {
        return consumerTmoTotCnt;
    }

    public TimeDltMetricItem getFileSyncTimeDltItem() {
        return fileSyncTimeDltItem;
    }

    public TimeDltMetricItem getZkSyncTimeDltItem() {
        return zkSyncTimeDltItem;
    }

    private MetricValues snapshotMetrics(boolean resetValue) {
        Map<String, Long> metricValues = new LinkedHashMap<>();
        fileSyncTimeDltItem.getProcTimeDltDuration(metricValues, resetValue);
        zkSyncTimeDltItem.getProcTimeDltDuration(metricValues, resetValue);
        metricValues.put(zkExceptionCnt.getName(), zkExceptionCnt.getValue(resetValue));
        metricValues.put(masterNoNodeCnt.getName(), masterNoNodeCnt.getValue(resetValue));
        metricValues.put(hbExceptionCnt.getName(), hbExceptionCnt.getValue(resetValue));
        metricValues.put(ioExceptionCnt.getName(), ioExceptionCnt.getValue(resetValue));
        metricValues.put(consumerOnlineCnt.getName(), consumerOnlineCnt.getValue(resetValue));
        metricValues.put(consumerTmoTotCnt.getName(), consumerTmoTotCnt.getValue(resetValue));
        if (resetValue) {
            long befTime = lastResetTime.getAndSet(System.currentTimeMillis());
            return new MetricValues(
                    DateTimeConvertUtils.ms2yyyyMMddHHmmss(befTime), metricValues);
        } else {
            return new MetricValues(
                    DateTimeConvertUtils.ms2yyyyMMddHHmmss(lastResetTime.get()), metricValues);
        }
    }

}

