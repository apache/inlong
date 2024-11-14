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

import org.apache.commons.lang.StringUtils;

public class MetricConfig {

    private static final long DEF_METRIC_REPORT_INTVL_MS = 60000L;
    private static final long MIN_METRIC_REPORT_INTVL_MS = 30000L;
    private static final long DEF_METRIC_DATE_FORMAT_MS = 60000L;
    private static final long MIN_METRIC_DATE_FORMAT_MS = 1L;
    private static final String DEF_METRIC_REPORT_GROUP_ID = "inlong_sla_metric";
    // metric enable
    private boolean enableMetric = false;
    // whether use groupId as key for metric, default is true
    private boolean useGroupIdAsKey = true;
    // whether use StreamId as key for metric, default is true
    private boolean useStreamIdAsKey = true;
    // whether use localIp as key for metric, default is true
    private boolean useLocalIpAsKey = true;
    // metric report interval, default is 1 mins in milliseconds.
    private long metricRptIntvlMs = DEF_METRIC_REPORT_INTVL_MS;
    // metric date format
    private long dateFormatIntvlMs = DEF_METRIC_DATE_FORMAT_MS;
    // metric groupId
    private String metricGroupId = DEF_METRIC_REPORT_GROUP_ID;

    public MetricConfig() {

    }

    public void setEnableMetric(boolean enableMetric) {
        this.enableMetric = enableMetric;
    }

    public boolean isEnableMetric() {
        return enableMetric;
    }

    public void setMetricKeyBuildParams(
            boolean useGroupIdAsKey, boolean useStreamIdAsKey, boolean useLocalIpAsKey) {
        this.useGroupIdAsKey = useGroupIdAsKey;
        this.useStreamIdAsKey = useStreamIdAsKey;
        this.useLocalIpAsKey = useLocalIpAsKey;
    }

    public boolean isUseGroupIdAsKey() {
        return useGroupIdAsKey;
    }

    public boolean isUseStreamIdAsKey() {
        return useStreamIdAsKey;
    }

    public boolean isUseLocalIpAsKey() {
        return useLocalIpAsKey;
    }

    public void setMetricRptIntvlMs(long metricRptIntvlMs) {
        if (metricRptIntvlMs >= MIN_METRIC_REPORT_INTVL_MS) {
            this.metricRptIntvlMs = metricRptIntvlMs;
        }
    }

    public long getMetricRptIntvlMs() {
        return metricRptIntvlMs;
    }

    public void setDateFormatIntvlMs(long dateFormatIntvlMs) {
        if (dateFormatIntvlMs >= MIN_METRIC_DATE_FORMAT_MS) {
            this.dateFormatIntvlMs = dateFormatIntvlMs;
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
            this.metricGroupId = metricGroupId;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MetricConfig{");
        sb.append("enableMetric=").append(enableMetric);
        sb.append(", useGroupIdAsKey=").append(useGroupIdAsKey);
        sb.append(", useStreamIdAsKey=").append(useStreamIdAsKey);
        sb.append(", useLocalIpAsKey=").append(useLocalIpAsKey);
        sb.append(", metricRptIntvlMs=").append(metricRptIntvlMs);
        sb.append(", dateFormatIntvlMs=").append(dateFormatIntvlMs);
        sb.append(", metricGroupId='").append(metricGroupId).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
