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

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerMetricsHolder {
    private static final Logger logger =
            LoggerFactory.getLogger(BrokerMetricsHolder.class);
    // Registration status indicator
    private static final AtomicBoolean registered = new AtomicBoolean(false);
    // broker metrics information
    private static final BrokerMetrics statsInfo = new BrokerMetrics();

    public static void registerMXBean() {
        if (!registered.compareAndSet(false, true)) {
            return;
        }
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName mxBeanName =
                    new ObjectName("org.apache.inlong.tubemq.server.broker:type=BrokerMetrics");
            mbs.registerMBean(statsInfo, mxBeanName);
        } catch (Exception ex) {
            logger.error("Register BrokerMXBean error: ", ex);
        }
    }

    public static void incConsumerCnt() {
        statsInfo.consumerOnlineCnt.incrementAndGet();
    }

    public static void decConsumerCnt(boolean isTimeout) {
        statsInfo.consumerOnlineCnt.decrementAndGet();
        if (isTimeout) {
            statsInfo.consumerTmoTotCnt.incrementAndGet();
        }
    }

    public static void incMasterNoNodeCnt() {
        statsInfo.masterNoNodeCnt.incrementAndGet();
    }

    public static void incHBExceptionCnt() {
        statsInfo.hbExceptionCnt.incrementAndGet();
    }

    public static void incIOExceptionCnt() {
        statsInfo.ioExceptionCnt.incrementAndGet();
    }

    public static void incZKExceptionCnt() {
        statsInfo.zkExceptionCnt.incrementAndGet();
    }

    public static void updSyncDataDurations(long dltTime) {
        statsInfo.fileSyncTimeDltItem.updProcTimeDlt(dltTime);
    }

    public static void updSyncZKDurations(long dltTime) {
        statsInfo.zkSyncTimeDltItem.updProcTimeDlt(dltTime);
    }

    public static BrokerMetrics getStatsInfo() {
        return statsInfo;
    }
}

