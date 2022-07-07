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

package org.apache.inlong.agent.plugin.metrics;

import org.apache.inlong.agent.utils.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalMetrics.class);

    // key: groupId_streamId
    private static final ConcurrentHashMap<String, PluginMetric> pluginMetrics = new ConcurrentHashMap<>();
    // key: sourceType_groupId_streamId
    private static final ConcurrentHashMap<String, SourceMetric> sourceMetrics = new ConcurrentHashMap<>();
    // key: sinkType_groupId_streamId
    private static final ConcurrentHashMap<String, SinkMetric> sinkMetrics = new ConcurrentHashMap<>();

    private static PluginMetric getPluginMetric(String tagName) {
        return pluginMetrics.computeIfAbsent(tagName, (key) -> addPluginMetric(tagName));
    }

    private static PluginMetric addPluginMetric(String tagName) {
        PluginMetric metric;
        if (ConfigUtil.isPrometheusEnabled()) {
            metric = new PluginPrometheusMetric(tagName);
        } else {
            metric = new PluginJmxMetric(tagName);
        }
        LOGGER.info("add {} pluginMetrics", tagName);
        return metric;
    }

    private static SourceMetric getSourceMetric(String tagName) {
        return sourceMetrics.computeIfAbsent(tagName, (key) -> addSourceMetric(tagName));
    }

    private static SourceMetric addSourceMetric(String tagName) {
        SourceMetric metric;
        if (ConfigUtil.isPrometheusEnabled()) {
            metric = new SourcePrometheusMetric(tagName);
        } else {
            metric = new SourceJmxMetric(tagName);
        }
        LOGGER.info("add {} sourceMetric", tagName);
        return metric;
    }

    private static SinkMetric getSinkMetric(String tagName) {
        return sinkMetrics.computeIfAbsent(tagName, (key) -> addSinkMetric(tagName));
    }

    private static SinkMetric addSinkMetric(String tagName) {
        SinkMetric metric;
        if (ConfigUtil.isPrometheusEnabled()) {
            metric = new SinkPrometheusMetric(tagName);
        } else {
            metric = new SinkJmxMetric(tagName);
        }
        LOGGER.info("add {} sinkMetric", tagName);
        return metric;
    }

    public static void incReadNum(String tagName) {
        getPluginMetric(tagName).incReadNum();
    }

    public static long getReadNum(String tagName) {
        return getPluginMetric(tagName).getReadNum();
    }

    public static void incSendNum(String tagName) {
        getPluginMetric(tagName).incSendNum();
    }

    public static long getSendNum(String tagName) {
        return getPluginMetric(tagName).getReadNum();
    }

    public static void incReadFailedNum(String tagName) {
        getPluginMetric(tagName).incReadFailedNum();
    }

    public static long getReadFailedNum(String tagName) {
        return getPluginMetric(tagName).getReadFailedNum();
    }

    public static void incSendFailedNum(String tagName) {
        getPluginMetric(tagName).incSendFailedNum();
    }

    public static long getSendFailedNum(String tagName) {
        return getPluginMetric(tagName).getSendFailedNum();
    }

    public static void incReadSuccessNum(String tagName) {
        getPluginMetric(tagName).incReadSuccessNum();
    }

    public static long getReadSuccessNum(String tagName) {
        return getPluginMetric(tagName).getReadSuccessNum();
    }

    public static void incSendSuccessNum(String tagName) {
        getPluginMetric(tagName).incSendSuccessNum();
    }

    public static void incSendSuccessNum(String tagName, int delta) {
        getPluginMetric(tagName).incSendSuccessNum(delta);
    }

    public static long getSendSuccessNum(String tagName) {
        return getPluginMetric(tagName).getSendSuccessNum();
    }

    public static void incSinkSuccessCount(String tagName) {
        getSinkMetric(tagName).incSinkSuccessCount();
    }

    public static long getSinkSuccessCount(String tagName) {
        return getSinkMetric(tagName).getSinkSuccessCount();
    }

    public static void incSinkFailCount(String tagName) {
        getSinkMetric(tagName).incSinkFailCount();
    }

    public static long getSinkFailCount(String tagName) {
        return getSinkMetric(tagName).getSinkFailCount();
    }

    public static void incSourceSuccessCount(String tagName) {
        getSourceMetric(tagName).incSourceSuccessCount();
    }

    public static long getSourceSuccessCount(String tagName) {
        return getSourceMetric(tagName).getSourceSuccessCount();
    }

    public static void incSourceFailCount(String tagName) {
        getSourceMetric(tagName).incSourceFailCount();
    }

    public static void showMemoryChannelStatics() {
        for (Entry<String, PluginMetric> entry : pluginMetrics.entrySet()) {
            LOGGER.info("tagName:{} ### readNum: {}, readSuccessNum: {}, readFailedNum: {}, sendSuccessNum: {}, "
                            + "sendFailedNum: {}", entry.getKey(), entry.getValue().getReadNum(),
                    entry.getValue().getReadSuccessNum(), entry.getValue().getReadFailedNum(),
                    entry.getValue().getSendSuccessNum(), entry.getValue().getSendFailedNum());
        }
    }

    public long getSourceFailCount(String tagName) {
        return getSourceMetric(tagName).getSourceFailCount();
    }
}
