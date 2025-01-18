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

package org.apache.inlong.sdk.dataproxy.metric;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class MetricConfig {

    private static final Logger logger = LoggerFactory.getLogger(MetricConfig.class);
    private static final long DEF_METRIC_OUTPUT_INTVL_MS = 60000L;
    private static final long MIN_METRIC_OUTPUT_INTVL_MS = 10000L;
    private static final long DEF_METRIC_OUTPUT_WARN_INT_MS = 10000L;
    private static final long DEF_METRIC_DATE_FORMAT_MS = 60000L;
    private static final String DEF_METRIC_REPORT_GROUP_ID = "inlong_sla_metric";
    // metric enable
    private boolean enableMetric = true;
    // whether use groupId as key for metric, default is true
    private boolean maskGroupId = false;
    // whether mask StreamId for metric, default is false
    private boolean maskStreamId = false;
    // metric report interval, default is 1 mins in milliseconds.
    private long metricOutIntvlMs = DEF_METRIC_OUTPUT_INTVL_MS;
    private long metricOutWarnIntMs = DEF_METRIC_OUTPUT_WARN_INT_MS;
    // metric date format
    private long dateFormatIntvlMs = DEF_METRIC_DATE_FORMAT_MS;
    private String metricGroupId = DEF_METRIC_REPORT_GROUP_ID;

    public MetricConfig() {

    }

    public void setEnableMetric(boolean enableMetric) {
        this.enableMetric = enableMetric;
    }

    public boolean isEnableMetric() {
        return enableMetric;
    }

    public void setMetricKeyMaskInfos(boolean maskGroupId, boolean maskStreamId) {
        this.maskGroupId = maskGroupId;
        this.maskStreamId = maskStreamId;
    }

    public boolean isMaskGroupId() {
        return maskGroupId;
    }

    public boolean isMaskStreamId() {
        return maskStreamId;
    }

    public void setMetricOutIntvlMs(long metricOutIntvlMs) {
        if (metricOutIntvlMs >= MIN_METRIC_OUTPUT_INTVL_MS) {
            this.metricOutIntvlMs = metricOutIntvlMs;
        }
    }

    public long getMetricOutIntvlMs() {
        return metricOutIntvlMs;
    }

    public long getMetricOutWarnIntMs() {
        return metricOutWarnIntMs;
    }

    public void setMetricOutWarnIntMs(long metricOutWarnIntMs) {
        if (metricOutWarnIntMs >= MIN_METRIC_OUTPUT_INTVL_MS) {
            this.metricOutWarnIntMs = metricOutWarnIntMs;
        }
    }

    public long getDateFormatIntvlMs() {
        return dateFormatIntvlMs;
    }

    public String getMetricGroupId() {
        return metricGroupId;
    }

    public void setMetricGroupId(String metricGroupId) {
        if (StringUtils.isNotBlank(metricGroupId)) {
            this.metricGroupId = metricGroupId.trim();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        MetricConfig that = (MetricConfig) o;
        return enableMetric == that.enableMetric
                && maskGroupId == that.maskGroupId
                && maskStreamId == that.maskStreamId
                && metricOutIntvlMs == that.metricOutIntvlMs
                && metricOutWarnIntMs == that.metricOutWarnIntMs
                && dateFormatIntvlMs == that.dateFormatIntvlMs
                && Objects.equals(metricGroupId, that.metricGroupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enableMetric, maskGroupId, maskStreamId,
                metricOutIntvlMs, dateFormatIntvlMs, metricOutWarnIntMs,
                metricGroupId);
    }

    @Override
    public MetricConfig clone() {
        try {
            return (MetricConfig) super.clone();
        } catch (Throwable ex) {
            logger.warn("Failed to clone MetricConfig", ex);
            return null;
        }
    }

    public void getSetting(StringBuilder strBuff) {
        strBuff.append("{enableMetric=").append(enableMetric)
                .append(", maskGroupId=").append(maskGroupId)
                .append(", maskStreamId=").append(maskStreamId)
                .append(", metricRptIntvlMs=").append(metricOutIntvlMs)
                .append(", metricOutWarnIntMs=").append(metricOutWarnIntMs)
                .append(", dateFormatIntvlMs=").append(dateFormatIntvlMs)
                .append(", metricGroupId='").append(metricGroupId)
                .append("'}");
    }
}
