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

package org.apache.inlong.agent.metrics.global;

import org.apache.inlong.agent.metrics.plugin.PluginMetric;
import org.apache.inlong.agent.metrics.sink.SinkMetric;
import org.apache.inlong.agent.metrics.source.SourceMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Global Metrics
 */
public abstract class GlobalMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalMetrics.class);

    // key: groupId_streamId
    private final ConcurrentHashMap<String, PluginMetric> pluginMetrics = new ConcurrentHashMap<>();
    // key: sourceType_groupId_streamId
    private final ConcurrentHashMap<String, SourceMetric> sourceMetrics = new ConcurrentHashMap<>();
    // key: sinkType_groupId_streamId
    private final ConcurrentHashMap<String, SinkMetric> sinkMetrics = new ConcurrentHashMap<>();

    private PluginMetric getPluginMetric(String tagName) {
        return pluginMetrics.computeIfAbsent(tagName, (key) -> addPluginMetric(tagName));
    }

    protected abstract PluginMetric addPluginMetric(String tagName);

    private SourceMetric getSourceMetric(String tagName) {
        return sourceMetrics.computeIfAbsent(tagName, (key) -> addSourceMetric(tagName));
    }

    protected abstract SourceMetric addSourceMetric(String tagName);

    private SinkMetric getSinkMetric(String tagName) {
        return sinkMetrics.computeIfAbsent(tagName, (key) -> addSinkMetric(tagName));
    }

    protected abstract SinkMetric addSinkMetric(String tagName);

    public void incReadNum(String tagName) {
        getPluginMetric(tagName).incReadNum();
    }

    public long getReadNum(String tagName) {
        return getPluginMetric(tagName).getReadNum();
    }

    public void incSendNum(String tagName) {
        getPluginMetric(tagName).incSendNum();
    }

    public long getSendNum(String tagName) {
        return getPluginMetric(tagName).getReadNum();
    }

    public void incReadFailedNum(String tagName) {
        getPluginMetric(tagName).incReadFailedNum();
    }

    public long getReadFailedNum(String tagName) {
        return getPluginMetric(tagName).getReadFailedNum();
    }

    public void incSendFailedNum(String tagName) {
        getPluginMetric(tagName).incSendFailedNum();
    }

    public long getSendFailedNum(String tagName) {
        return getPluginMetric(tagName).getSendFailedNum();
    }

    public void incReadSuccessNum(String tagName) {
        getPluginMetric(tagName).incReadSuccessNum();
    }

    public long getReadSuccessNum(String tagName) {
        return getPluginMetric(tagName).getReadSuccessNum();
    }

    public void incSendSuccessNum(String tagName) {
        getPluginMetric(tagName).incSendSuccessNum();
    }

    public void incSendSuccessNum(String tagName, int delta) {
        getPluginMetric(tagName).incSendSuccessNum(delta);
    }

    public long getSendSuccessNum(String tagName) {
        return getPluginMetric(tagName).getSendSuccessNum();
    }

    public void incSinkSuccessCount(String tagName) {
        getSinkMetric(tagName).incSinkSuccessCount();
    }

    public long getSinkSuccessCount(String tagName) {
        return getSinkMetric(tagName).getSinkSuccessCount();
    }

    public void incSinkFailCount(String tagName) {
        getSinkMetric(tagName).incSinkFailCount();
    }

    public long getSinkFailCount(String tagName) {
        return getSinkMetric(tagName).getSinkFailCount();
    }

    public void incSourceSuccessCount(String tagName) {
        getSourceMetric(tagName).incSourceSuccessCount();
    }

    public long getSourceSuccessCount(String tagName) {
        return getSourceMetric(tagName).getSourceSuccessCount();
    }

    public void incSourceFailCount(String tagName) {
        getSourceMetric(tagName).incSourceFailCount();
    }

    public void showMemoryChannelStatics() {
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
