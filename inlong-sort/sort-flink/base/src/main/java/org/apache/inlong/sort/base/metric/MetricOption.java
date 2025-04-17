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

package org.apache.inlong.sort.base.metric;

import org.apache.inlong.sort.util.AuditUtils;

import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.inlong.sort.base.Constants.AUDIT_SORT_INPUT;
import static org.apache.inlong.sort.base.Constants.DELIMITER;
import static org.apache.inlong.sort.base.Constants.GROUP_ID;
import static org.apache.inlong.sort.base.Constants.STREAM_ID;

public class MetricOption implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(MetricOption.class);

    private static final long serialVersionUID = 1L;

    private Map<String, String> labels;
    private Set<String> ipPortSet;
    private String ipPorts;
    private RegisteredMetric registeredMetric;
    private long initRecords;
    private long initBytes;
    private long initDirtyRecords;
    private long initDirtyBytes;
    private long readPhase;
    private List<Integer> inlongAuditKeys;
    private Map<RowKind, Integer> inlongChangelogAuditKeys;

    private MetricOption(
            Map<String, String> labels,
            @Nullable String inlongAudit,
            RegisteredMetric registeredMetric,
            long initRecords,
            long initBytes,
            Long initDirtyRecords,
            Long initDirtyBytes,
            Long readPhase,
            List<Integer> inlongAuditKeys,
            Map<RowKind, Integer> inlongChangelogAuditKeys,
            Set<String> ipPortSet) {
        this.initRecords = initRecords;
        this.initBytes = initBytes;
        this.initDirtyRecords = initDirtyRecords;
        this.initDirtyBytes = initDirtyBytes;
        this.readPhase = readPhase;
        this.labels = labels;
        this.ipPorts = inlongAudit;
        this.inlongAuditKeys = inlongAuditKeys;
        this.ipPortSet = ipPortSet;
        this.registeredMetric = registeredMetric;
        this.inlongChangelogAuditKeys = inlongChangelogAuditKeys;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public HashSet<String> getIpPortSet() {
        return new HashSet<>(ipPortSet);
    }

    public Optional<String> getIpPorts() {
        return Optional.ofNullable(ipPorts);
    }

    public RegisteredMetric getRegisteredMetric() {
        return registeredMetric;
    }

    public long getInitRecords() {
        return initRecords;
    }

    public long getInitBytes() {
        return initBytes;
    }

    public void setInitRecords(long initRecords) {
        this.initRecords = initRecords;
    }

    public void setInitBytes(long initBytes) {
        this.initBytes = initBytes;
    }

    public long getInitDirtyRecords() {
        return initDirtyRecords;
    }

    public void setInitDirtyRecords(long initDirtyRecords) {
        this.initDirtyRecords = initDirtyRecords;
    }

    public List<Integer> getInlongAuditKeys() {
        return inlongAuditKeys;
    }

    public Map<RowKind, Integer> getInlongChangelogAuditKeys() {
        return inlongChangelogAuditKeys;
    }

    public long getInitDirtyBytes() {
        return initDirtyBytes;
    }

    public void setInitDirtyBytes(long initDirtyBytes) {
        this.initDirtyBytes = initDirtyBytes;
    }

    public long getReadPhase() {
        return readPhase;
    }

    public void setReadPhase(long readPhase) {
        this.readPhase = readPhase;
    }

    public static Builder builder() {
        return new Builder();
    }

    public enum RegisteredMetric {
        ALL,
        NORMAL,
        DIRTY
    }

    public static class Builder {

        private String inlongLabels;
        private String inlongAudit;
        private String inlongAuditKeys;
        private String inlongChangelogAuditKeys;
        private RegisteredMetric registeredMetric = RegisteredMetric.ALL;
        private long initRecords = 0L;
        private long initBytes = 0L;
        private Long initDirtyRecords = 0L;
        private Long initDirtyBytes = 0L;
        private long initReadPhase = 0L;

        private Builder() {
        }

        public MetricOption.Builder withInlongLabels(String inlongLabels) {
            this.inlongLabels = inlongLabels;
            return this;
        }

        public MetricOption.Builder withAuditAddress(String inlongAudit) {
            this.inlongAudit = inlongAudit;
            return this;
        }

        public MetricOption.Builder withAuditKeys(String inlongAuditIds) {
            this.inlongAuditKeys = inlongAuditIds;
            return this;
        }

        public MetricOption.Builder withChangelogAuditKeys(String inlongChangelogAuditKeys) {
            this.inlongChangelogAuditKeys = inlongChangelogAuditKeys;
            return this;
        }

        public MetricOption.Builder withRegisterMetric(RegisteredMetric registeredMetric) {
            this.registeredMetric = registeredMetric;
            return this;
        }

        public MetricOption.Builder withInitRecords(long initRecords) {
            this.initRecords = initRecords;
            return this;
        }

        public MetricOption.Builder withInitBytes(long initBytes) {
            this.initBytes = initBytes;
            return this;
        }

        public MetricOption.Builder withInitDirtyRecords(Long initDirtyRecords) {
            this.initDirtyRecords = initDirtyRecords;
            return this;
        }

        public MetricOption.Builder withInitDirtyBytes(Long initDirtyBytes) {
            this.initDirtyBytes = initDirtyBytes;
            return this;
        }

        public MetricOption.Builder withInitReadPhase(Long initReadPhase) {
            this.initReadPhase = initReadPhase;
            return this;
        }

        public MetricOption build() {
            if (inlongAudit == null && inlongLabels == null) {
                LOG.warn("The property 'metrics.audit.proxy.hosts and inlong.metric.labels' has not been set," +
                        " the program will not open audit function");
                return null;
            }

            Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(inlongLabels),
                    "Inlong labels must be set for register metric.");
            String[] inLongLabelArray = inlongLabels.split(DELIMITER);
            Preconditions.checkArgument(Stream.of(inLongLabelArray).allMatch(label -> label.contains("=")),
                    "InLong metric label format must be xxx=xxx");
            Map<String, String> labels = new LinkedHashMap<>();
            Stream.of(inLongLabelArray).forEach(label -> {
                String key = label.substring(0, label.indexOf('='));
                String value = label.substring(label.indexOf('=') + 1);
                labels.put(key, value);
            });

            List<Integer> inlongAuditKeysList = null;
            Set<String> ipPortSet = null;
            Map<RowKind, Integer> inlongChangelogAuditKeysMap = null;

            if (inlongAudit != null) {
                Preconditions.checkArgument(labels.containsKey(GROUP_ID) && labels.containsKey(STREAM_ID),
                        "The groupId and streamId must be set when enable inlong audit collect.");

                if (inlongAuditKeys == null) {
                    LOG.warn("The inlongAuditKeys should be set when enable inlong audit collect, "
                            + "fallback to use id {} as audit key", AUDIT_SORT_INPUT);
                    inlongAuditKeys = AUDIT_SORT_INPUT;
                }

                inlongAuditKeysList = AuditUtils.extractAuditKeys(inlongAuditKeys);
                ipPortSet = AuditUtils.extractAuditIpPorts(inlongAudit);
                inlongChangelogAuditKeysMap = AuditUtils.extractChangelogAuditKeyMap(inlongChangelogAuditKeys);

            }

            return new MetricOption(labels, inlongAudit, registeredMetric, initRecords, initBytes,
                    initDirtyRecords, initDirtyBytes, initReadPhase, inlongAuditKeysList, inlongChangelogAuditKeysMap,
                    ipPortSet);
        }
    }
}
