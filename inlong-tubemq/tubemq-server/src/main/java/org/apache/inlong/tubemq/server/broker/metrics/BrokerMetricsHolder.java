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

    private static final AtomicBoolean registered = new AtomicBoolean(false);
    public static final BrokerMetrics METRICS = new BrokerMetrics();

    public static void registerMXBean() {
        if (!registered.compareAndSet(false, true)) {
            return;
        }
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName mxBeanName =
                    new ObjectName("org.apache.inlong.tubemq.server.broker:type=brokerMetrics");
            mbs.registerMBean(METRICS, mxBeanName);
        } catch (Exception ex) {
            logger.error("Register BrokerMXBean error: ", ex);
        }
    }
}

