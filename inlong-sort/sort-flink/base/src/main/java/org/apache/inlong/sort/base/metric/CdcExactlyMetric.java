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

import org.apache.inlong.audit.AuditReporterImpl;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.Map;

import static org.apache.inlong.audit.consts.ConfigConstants.DEFAULT_AUDIT_TAG;
import static org.apache.inlong.common.constant.Constants.DEFAULT_AUDIT_VERSION;
import static org.apache.inlong.sort.base.Constants.GROUP_ID;
import static org.apache.inlong.sort.base.Constants.STREAM_ID;
import static org.apache.inlong.sort.base.util.CalculateObjectSizeUtils.getDataSize;

@Slf4j
public class CdcExactlyMetric implements Serializable, SourceMetricsReporter {

    private final Map<String, String> labels;
    private final Map<RowKind, Integer> auditKeyMap;
    private final String groupId;
    private final String streamId;

    private AuditReporterImpl auditReporter;
    private Long currentCheckpointId = 0L;
    private Long lastCheckpointId = 0L;

    public CdcExactlyMetric(MetricOption option) {
        this.labels = option.getLabels();
        this.groupId = labels.get(GROUP_ID);
        this.streamId = labels.get(STREAM_ID);

        if (option.getIpPorts().isPresent()) {
            auditReporter = new AuditReporterImpl();
            auditReporter.setAutoFlush(false);
            auditReporter.setAuditProxy(option.getIpPortSet());
        }
        auditKeyMap = option.getInlongChangelogAuditKeys();
        log.info("CdcExactlyMetric init, groupId: {}, streamId: {}, audit key: {}", groupId, streamId, auditKeyMap);
    }

    @Override
    public void outputMetricsWithEstimate(Object data, long dataTime) {
        long size = getDataSize(data);
        if (data instanceof RowData) {
            RowData rowData = (RowData) data;
            RowKind rowKind = rowData.getRowKind();
            Integer key = auditKeyMap.get(rowKind);
            outputMetrics(1, size, dataTime, key);
        } else {
            outputMetrics(1, size, dataTime, auditKeyMap.get(RowKind.INSERT));
        }
    }

    public void outputMetrics(long rowCountSize, long rowDataSize, long dataTime, Integer key) {
        if (auditReporter != null && key != null) {
            auditReporter.add(
                    this.currentCheckpointId,
                    key,
                    DEFAULT_AUDIT_TAG,
                    groupId,
                    streamId,
                    dataTime,
                    rowCountSize,
                    rowDataSize,
                    DEFAULT_AUDIT_VERSION);
        }
    }

    public void updateLastCheckpointId(Long checkpointId) {
        lastCheckpointId = checkpointId;
    }

    public void updateCurrentCheckpointId(Long checkpointId) {
        currentCheckpointId = checkpointId;
    }

    public void flushAudit() {
        if (auditReporter != null) {
            auditReporter.flush(lastCheckpointId);
        }
    }
}
