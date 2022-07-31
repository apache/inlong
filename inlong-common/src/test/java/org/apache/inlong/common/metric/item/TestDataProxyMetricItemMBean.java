/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.common.metric.item;

import org.apache.inlong.common.metric.MetricRegister;
import org.apache.inlong.common.metric.set.DataProxyMetricItem;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestMetricItem
 */
public class TestDataProxyMetricItemMBean {

    public static final String MODULE = "DataProxy";
    public static final String TAG = "dataproxy";
    private static DataProxyMetricItem item;

    /**
     * setup
     */
    @BeforeClass
    public static void setup() {
        item = new DataProxyMetricItem();
        item.setId = MODULE;
        item.containerName = TAG;
        MetricRegister.register(item);
    }

    /**
     * testResult
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testResult() throws Exception {
        while (true) {
            Thread.sleep(1000);
        }
        // increase
        // item.readSuccessCount.incrementAndGet();
        // item.readFailSize.addAndGet(100);
        // item.sendCount.addAndGet(2);
        // //
        // final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        // StringBuilder beanName = new StringBuilder();
        // beanName.append(MetricRegister.JMX_DOMAIN).append(MetricItemMBean.DOMAIN_SEPARATOR)
        //         .append("type=").append(MetricUtils.getDomain(DataProxyMetricItem.class))
        //         .append(MetricItemMBean.PROPERTY_SEPARATOR)
        //         .append("module=").append(MODULE).append(MetricItemMBean.PROPERTY_SEPARATOR)
        //         .append("tag=").append(TAG);
        // String strBeanName = beanName.toString();
        // ObjectName objName = new ObjectName(strBeanName);
        // {
        //     Map<String, String> dimensions = (Map<String, String>) mbs.getAttribute(objName,
        //             MetricItemMBean.ATTRIBUTE_DIMENSIONS);
        //     assertEquals(MODULE, dimensions.get("module"));
        //     assertEquals(TAG, dimensions.get("tag"));
        //     Map<String, MetricValue> metricMap = (Map<String, MetricValue>) mbs.invoke(objName,
        //             MetricItemMBean.METHOD_SNAPSHOT, null, null);
        //     assertEquals(1, metricMap.get("readNum").value);
        //     assertEquals(100, metricMap.get("sendNum").value);
        //     assertEquals(2, metricMap.get("runningTasks").value);
        // }
        // // increase
        // item.readSuccessCount.incrementAndGet();
        // item.readFailSize.addAndGet(200);
        // item.sendCount.addAndGet(4);
        // {
        //     Map<String, String> dimensions = (Map<String, String>) mbs.getAttribute(objName,
        //             MetricItemMBean.ATTRIBUTE_DIMENSIONS);
        //     assertEquals(MODULE, dimensions.get("module"));
        //     assertEquals(TAG, dimensions.get("tag"));
        //     Map<String, MetricValue> metricMap = (Map<String, MetricValue>) mbs.invoke(objName,
        //             MetricItemMBean.METHOD_SNAPSHOT, null, null);
        //     assertEquals(1, metricMap.get("readNum").value);
        //     assertEquals(100, metricMap.get("sendNum").value);
        //     assertEquals(4, metricMap.get("runningTasks").value);
        // }
    }
}
