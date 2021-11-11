/**
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

package org.apache.inlong.commons.config.metrics.item;

import static org.junit.Assert.assertEquals;

import java.lang.management.ManagementFactory;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.inlong.commons.config.metrics.MetricItemMBean;
import org.apache.inlong.commons.config.metrics.MetricRegister;
import org.apache.inlong.commons.config.metrics.MetricUtils;
import org.apache.inlong.commons.config.metrics.MetricValue;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * TestMetricItem
 */
public class TestMetricItemMBean {

    public static final String MODULE = "Plugin";
    public static final String ASPECT = "PluginSummary";
    public static final String TAG = "agent1";
    private static AgentMetricItem item;

    /**
     * setup
     */
    @BeforeClass
    public static void setup() {
        item = new AgentMetricItem();
        item.module = MODULE;
        item.aspect = ASPECT;
        item.tag = TAG;
        MetricRegister.register(item);
    }

    /**
     * testResult
     * 
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testResult() throws Exception {
        // increase
        item.readNum.incrementAndGet();
        item.sendNum.addAndGet(100);
        item.runningTasks.addAndGet(2);
        //
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        StringBuilder beanName = new StringBuilder();
        beanName.append(MetricUtils.getDomain(AgentMetricItem.class)).append(MetricItemMBean.DOMAIN_SEPARATOR)
                .append("module=").append(MODULE).append(MetricItemMBean.PROPERTY_SEPARATOR)
                .append("aspect=").append(ASPECT).append(MetricItemMBean.PROPERTY_SEPARATOR)
                .append("tag=").append(TAG);
        String strBeanName = beanName.toString();
        ObjectName objName = new ObjectName(strBeanName);
        {
            Map<String, String> dimensions = (Map<String, String>) mbs.getAttribute(objName, "Dimensions");
            Map<String, MetricValue> metricMap = (Map<String, MetricValue>) mbs.invoke(objName, "snapshot", null, null);
            assertEquals(MODULE, dimensions.get("module"));
            assertEquals(ASPECT, dimensions.get("aspect"));
            assertEquals(TAG, dimensions.get("tag"));
            assertEquals(1, metricMap.get("readNum").value);
            assertEquals(100, metricMap.get("sendNum").value);
            assertEquals(2, metricMap.get("runningTasks").value);
        }
        // increase
        item.readNum.incrementAndGet();
        item.sendNum.addAndGet(100);
        item.runningTasks.addAndGet(2);
        {
            Map<String, String> dimensions = (Map<String, String>) mbs.getAttribute(objName, "Dimensions");
            Map<String, MetricValue> metricMap = (Map<String, MetricValue>) mbs.invoke(objName, "snapshot", null, null);
            assertEquals(MODULE, dimensions.get("module"));
            assertEquals(ASPECT, dimensions.get("aspect"));
            assertEquals(TAG, dimensions.get("tag"));
            assertEquals(1, metricMap.get("readNum").value);
            assertEquals(100, metricMap.get("sendNum").value);
            assertEquals(4, metricMap.get("runningTasks").value);
        }
    }
}
