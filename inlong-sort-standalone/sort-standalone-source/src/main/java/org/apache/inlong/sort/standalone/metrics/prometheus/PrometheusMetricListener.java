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

package org.apache.inlong.sort.standalone.metrics.prometheus;

import static org.apache.inlong.commons.config.metrics.MetricItemMBean.DOMAIN_SEPARATOR;
import static org.apache.inlong.commons.config.metrics.MetricRegister.JMX_DOMAIN;
import static org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder.KEY_CLUSTER_ID;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.inlong.commons.config.metrics.MetricRegister;
import org.apache.inlong.commons.config.metrics.MetricValue;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.metrics.MetricItemValue;
import org.apache.inlong.sort.standalone.metrics.MetricListener;
import org.apache.inlong.sort.standalone.metrics.SortMetricItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * PrometheusMetricListener
 */
public class PrometheusMetricListener implements MetricListener {

    public static final Logger LOG = LoggerFactory.getLogger(MetricRegister.class);

    private SortMetricItem metricItem;
    private Map<String, AtomicLong> metricValueMap = new ConcurrentHashMap<>();

    /**
     * Constructor
     */
    public PrometheusMetricListener() {
        this.metricItem = new SortMetricItem();
        this.metricItem.clusterId = CommonPropertiesHolder.getString(KEY_CLUSTER_ID);
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        StringBuilder beanName = new StringBuilder();
        beanName.append(JMX_DOMAIN).append(DOMAIN_SEPARATOR).append("type=DataProxyCounter");
        String strBeanName = beanName.toString();
        try {
            ObjectName objName = new ObjectName(strBeanName);
            mbs.registerMBean(metricItem, objName);
        } catch (Exception ex) {
            LOG.error("exception while register mbean:{},error:{}", strBeanName, ex.getMessage());
            LOG.error(ex.getMessage(), ex);
        }
        // prepare metric value map
        metricValueMap.put(SortMetricItem.M_READ_SUCCESS_COUNT, metricItem.readSuccessCount);
        metricValueMap.put(SortMetricItem.M_READ_SUCCESS_SIZE, metricItem.readSuccessSize);
        metricValueMap.put(SortMetricItem.M_READ_FAIL_COUNT, metricItem.readFailCount);
        metricValueMap.put(SortMetricItem.M_READ_FAIL_SIZE, metricItem.readFailSize);
        //
        metricValueMap.put(SortMetricItem.M_SEND_COUNT, metricItem.sendCount);
        metricValueMap.put(SortMetricItem.M_SEND_SIZE, metricItem.sendSize);
        //
        metricValueMap.put(SortMetricItem.M_SEND_SUCCESS_COUNT, metricItem.sendSuccessCount);
        metricValueMap.put(SortMetricItem.M_SEND_SUCCESS_SIZE, metricItem.sendSuccessSize);
        metricValueMap.put(SortMetricItem.M_SEND_FAIL_COUNT, metricItem.sendFailCount);
        metricValueMap.put(SortMetricItem.M_SEND_FAIL_SIZE, metricItem.sendFailSize);
        //
        metricValueMap.put(SortMetricItem.M_SINK_DURATION, metricItem.sinkDuration);
        metricValueMap.put(SortMetricItem.M_NODE_DURATION, metricItem.nodeDuration);
        metricValueMap.put(SortMetricItem.M_WHOLE_DURATION, metricItem.wholeDuration);
    }

    /**
     * snapshot
     * 
     * @param domain
     * @param itemValues
     */
    @Override
    public void snapshot(String domain, List<MetricItemValue> itemValues) {
        for (MetricItemValue itemValue : itemValues) {
            for (Entry<String, MetricValue> entry : itemValue.getMetrics().entrySet()) {
                String fieldName = entry.getValue().name;
                AtomicLong metricValue = this.metricValueMap.get(fieldName);
                if (metricValue != null) {
                    long fieldValue = entry.getValue().value;
                    metricValue.addAndGet(fieldValue);
                }
            }
        }
    }

}
