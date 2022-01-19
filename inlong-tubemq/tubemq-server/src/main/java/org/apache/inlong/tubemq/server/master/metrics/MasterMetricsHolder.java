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

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterMetricsHolder {
    private static final Logger logger =
            LoggerFactory.getLogger(MasterMetricsHolder.class);
    // Registration status indicator
    private static final AtomicBoolean registered =
            new AtomicBoolean(false);
    // master metrics information
    private static final MasterMetrics statsInfo = new MasterMetrics();

    public static void registerMXBean() {
        if (!registered.compareAndSet(false, true)) {
            return;
        }
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName mxBeanName =
                    new ObjectName("org.apache.inlong.tubemq.server.master:type=MasterMetrics");
            mbs.registerMBean(statsInfo, mxBeanName);
        } catch (Exception ex) {
            logger.error("Register MasterMXBean error: ", ex);
        }
    }

    public static void incConsumerCnt(boolean isGroupEmpty, boolean isCltBal) {
        statsInfo.consumerOnlineCnt.incrementAndGet();
        if (isGroupEmpty) {
            statsInfo.consumeGroupCnt.incrementAndGet();
            if (isCltBal) {
                statsInfo.cltBalConsumeGroupCnt.incrementAndGet();
            }
        }
    }

    public static void decConsumerCnt(boolean isTimeout,
                                      boolean isGroupEmpty,
                                      boolean isCltBal) {
        statsInfo.consumerOnlineCnt.decrementAndGet();
        if (isTimeout) {
            statsInfo.consumerTmoTotCnt.incrementAndGet();
        }
        if (isGroupEmpty) {
            decConsumeGroupCnt(isTimeout, isCltBal);
        }
    }

    public static void decConsumeGroupCnt(boolean isTimeout, boolean isCltBal) {
        statsInfo.consumeGroupCnt.decrementAndGet();
        if (isTimeout) {
            statsInfo.consumeGroupTmoTotCnt.incrementAndGet();
        }
        if (isCltBal) {
            statsInfo.cltBalConsumeGroupCnt.decrementAndGet();
            if (isTimeout) {
                statsInfo.cltBalGroupTmototCnt.incrementAndGet();
            }
        }
    }

    public static void incProducerCnt() {
        statsInfo.producerOnlineCnt.incrementAndGet();
    }

    public static void decProducerCnt(boolean isTimeout) {
        statsInfo.producerOnlineCnt.decrementAndGet();
        if (isTimeout) {
            statsInfo.producerTmoTotCnt.incrementAndGet();
        }
    }

    public static void incSvrBalDisConConsumerCnt() {
        statsInfo.svrBalDisConEventConsumerCnt.incrementAndGet();
    }

    public static void decSvrBalDisConConsumerCnt() {
        statsInfo.svrBalDisConEventConsumerCnt.decrementAndGet();
    }

    public static void incSvrBalConEventConsumerCnt() {
        statsInfo.svrBalConEventConsumerCnt.incrementAndGet();
    }

    public static void decSvrBalConEventConsumerCnt() {
        statsInfo.svrBalConEventConsumerCnt.decrementAndGet();
    }

    public static void incBrokerConfigCnt() {
        statsInfo.brokerConfigCnt.incrementAndGet();
    }

    public static void decBrokerConfigCnt() {
        statsInfo.brokerConfigCnt.decrementAndGet();
    }

    public static void incBrokerOnlineCnt() {
        statsInfo.brokerOnlineCnt.incrementAndGet();
    }

    public static void decBrokerOnlineCnt(boolean isTimeout) {
        statsInfo.brokerOnlineCnt.decrementAndGet();
        if (isTimeout) {
            statsInfo.brokerTmoTotCnt.incrementAndGet();
        }
    }

    public static void incBrokerAbnormalCnt() {
        statsInfo.brokerAbnCurCnt.incrementAndGet();
        statsInfo.brokerAbnTotCnt.incrementAndGet();
    }

    public static void decBrokerAbnormalCnt() {
        statsInfo.brokerAbnCurCnt.decrementAndGet();
    }

    public static void incBrokerForbiddenCnt() {
        statsInfo.brokerFbdCurCnt.incrementAndGet();
        statsInfo.brokerFbdTotCnt.incrementAndGet();
    }

    public static void decBrokerForbiddenCnt() {
        statsInfo.brokerFbdCurCnt.decrementAndGet();
    }

    public static void updSvrBalanceDurations(long dltTime) {
        statsInfo.svrBalDltItem.updProcTimeDlt(dltTime);
    }

    public static void updSvrBalResetDurations(long dltTime) {
        statsInfo.svrBalResetDltItem.updProcTimeDlt(dltTime);
    }

    public static MasterMetrics getStatsInfo() {
        return statsInfo;
    }
}

