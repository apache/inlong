/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.inlong.sort.base.metric.sub;

import static org.apache.inlong.sort.base.Constants.DELIMITER;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_IN;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_IN;
import static org.apache.inlong.sort.base.Constants.READ_PHASE;

import com.google.common.collect.Maps;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.inlong.sort.base.Constants;
import org.apache.inlong.sort.base.enums.ReadPhase;
import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.MetricOption.RegisteredMetric;
import org.apache.inlong.sort.base.metric.MetricState;
import org.apache.inlong.sort.base.metric.SourceMetricData;
import org.apache.inlong.sort.base.metric.phase.ReadPhaseMetricData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection class for handling sub metrics of table schema type
 */
public class SourceTableMetricData extends SourceMetricData implements SourceSubMetricData {

    public static final Logger LOGGER = LoggerFactory.getLogger(SourceTableMetricData.class);

    /**
     * The read phase metric container of source metric data
     */
    private final Map<ReadPhase, ReadPhaseMetricData> readPhaseMetricDataMap = Maps.newHashMap();
    /**
     * The sub source metric data container of source metric data
     */
    private final Map<String, SourceMetricData> subSourceMetricMap = Maps.newHashMap();

    public SourceTableMetricData(MetricOption option, MetricGroup metricGroup) {
        super(option, metricGroup);
    }

    /**
     * register sub source metrics group from metric state
     *
     * @param metricState MetricState
     */
    public void registerSubMetricsGroup(MetricState metricState) {
        if (metricState == null) {
            return;
        }
        // register source read phase metric
        Stream.of(ReadPhase.values()).forEach(readPhase -> {
            Long readPhaseMetricValue = metricState.getMetricValue(readPhase.getPhase());
            if (readPhaseMetricValue > 0) {
                outputReadPhaseMetrics(metricState, readPhase);
            }
        });

        // register sub source metric data
        if (metricState.getSubMetricStateMap() == null) {
            return;
        }
        Map<String, MetricState> subMetricStateMap = metricState.getSubMetricStateMap();
        for (Entry<String, MetricState> subMetricStateEntry : subMetricStateMap.entrySet()) {
            String[] schemaInfoArray = parseSchemaIdentify(subMetricStateEntry.getKey());
            final MetricState subMetricState = subMetricStateEntry.getValue();
            SourceMetricData subSourceMetricData = buildSubSourceMetricData(schemaInfoArray,
                    subMetricState, this);
            subSourceMetricMap.put(subMetricStateEntry.getKey(), subSourceMetricData);
        }
        LOGGER.info("register subMetricsGroup from metricState,sub metric map size:{}", subSourceMetricMap.size());
    }

    /**
     * build sub source metric data
     *
     * @param schemaInfoArray source record schema info
     * @param sourceMetricData source metric data
     * @return sub source metric data
     */
    private SourceMetricData buildSubSourceMetricData(String[] schemaInfoArray, SourceMetricData sourceMetricData) {
        return buildSubSourceMetricData(schemaInfoArray, null, sourceMetricData);
    }

    /**
     * build sub source metric data
     *
     * @param schemaInfoArray the schema info array of record
     * @param subMetricState sub metric state
     * @param sourceMetricData source metric data
     * @return sub source metric data
     */
    private SourceMetricData buildSubSourceMetricData(String[] schemaInfoArray, MetricState subMetricState,
            SourceMetricData sourceMetricData) {
        if (sourceMetricData == null || schemaInfoArray == null) {
            return null;
        }
        // build sub source metric data
        Map<String, String> labels = sourceMetricData.getLabels();
        String metricGroupLabels = labels.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(DELIMITER));
        StringBuilder labelBuilder = new StringBuilder(metricGroupLabels);
        labelBuilder.append(DELIMITER).append(Constants.DATABASE_NAME).append("=").append(schemaInfoArray[0])
                .append(DELIMITER).append(Constants.TABLE_NAME).append("=").append(schemaInfoArray[1]);

        MetricOption metricOption = MetricOption.builder()
                .withInitRecords(subMetricState != null ? subMetricState.getMetricValue(NUM_RECORDS_IN) : 0L)
                .withInitBytes(subMetricState != null ? subMetricState.getMetricValue(NUM_BYTES_IN) : 0L)
                .withInlongLabels(labelBuilder.toString())
                .withRegisterMetric(RegisteredMetric.ALL)
                .build();
        return new SourceTableMetricData(metricOption, sourceMetricData.getMetricGroup());
    }

    /**
     * build record schema identify,in the form of database.table
     *
     * @param database the database name of record
     * @param table the table name of record
     * @return the record schema identify
     */
    public String buildSchemaIdentify(String database, String table) {
        return database + Constants.SEMICOLON + table;
    }

    /**
     * parse record schema identify
     *
     * @param schemaIdentify the schema identify of record
     * @return the record schema identify array,String[]{database,table}
     */
    public String[] parseSchemaIdentify(String schemaIdentify) {
        return schemaIdentify.split(Constants.SPILT_SEMICOLON);
    }

    /**
     * output metrics with estimate
     *
     * @param database the database name of record
     * @param table the table name of record
     * @param isSnapshotRecord is it snapshot record
     * @param data the data of record
     */
    public void outputMetricsWithEstimate(String database, String table, boolean isSnapshotRecord, Object data) {
        if (StringUtils.isBlank(database) || StringUtils.isBlank(table)) {
            outputMetricsWithEstimate(data);
            return;
        }
        String identify = buildSchemaIdentify(database, table);
        SourceMetricData subSourceMetricData;
        if (subSourceMetricMap.containsKey(identify)) {
            subSourceMetricData = subSourceMetricMap.get(identify);
        } else {
            subSourceMetricData = buildSubSourceMetricData(new String[]{database, table}, this);
            subSourceMetricMap.put(identify, subSourceMetricData);
        }
        // source metric and sub source metric output metrics
        long rowCountSize = 1L;
        long rowDataSize = data.toString().getBytes(StandardCharsets.UTF_8).length;
        this.outputMetrics(rowCountSize, rowDataSize);
        subSourceMetricData.outputMetrics(rowCountSize, rowDataSize);

        // output read phase metric
        outputReadPhaseMetrics((isSnapshotRecord) ? ReadPhase.SNAPSHOT_PHASE : ReadPhase.INCREASE_PHASE);
    }

    /**
     * output read phase metric
     *
     * @param readPhase the readPhase of record
     */
    public void outputReadPhaseMetrics(ReadPhase readPhase) {
        outputReadPhaseMetrics(null, readPhase);
    }

    /**
     * output read phase metric
     *
     * @param metricState the metric state of record
     * @param readPhase the readPhase of record
     */
    public void outputReadPhaseMetrics(MetricState metricState, ReadPhase readPhase) {
        ReadPhaseMetricData readPhaseMetricData = readPhaseMetricDataMap.get(readPhase);
        if (readPhaseMetricData == null) {
            // build read phase metric data
            String metricGroupLabels = getLabels().entrySet().stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.joining(DELIMITER));

            MetricOption metricOption = MetricOption.builder()
                    .withInitReadPhase(metricState != null ? metricState.getMetricValue(readPhase.getPhase()) : 0L)
                    .withInlongLabels(metricGroupLabels + DELIMITER + READ_PHASE + "=" + readPhase.getCode())
                    .withRegisterMetric(RegisteredMetric.ALL)
                    .build();
            readPhaseMetricData = new ReadPhaseMetricData(metricOption, getMetricGroup());
            readPhaseMetricDataMap.put(readPhase, readPhaseMetricData);
        }
        readPhaseMetricData.outputMetrics();
    }

    @Override
    public Map<ReadPhase, ReadPhaseMetricData> getReadPhaseMetricMap() {
        return readPhaseMetricDataMap;
    }

    @Override
    public Map<String, SourceMetricData> getSubSourceMetricMap() {
        return this.subSourceMetricMap;
    }

    @Override
    public String toString() {
        return "SourceTableMetricData{"
                + "readPhaseMetricDataMap=" + readPhaseMetricDataMap
                + ", subSourceMetricMap=" + subSourceMetricMap
                + '}';
    }
}