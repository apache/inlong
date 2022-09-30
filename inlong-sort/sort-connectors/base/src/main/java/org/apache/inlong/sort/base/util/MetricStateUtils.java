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

package org.apache.inlong.sort.base.util;

import org.apache.flink.api.common.state.ListState;
import org.apache.inlong.sort.base.metric.MetricState;
import org.apache.inlong.sort.base.metric.SinkMetricData;
import org.apache.inlong.sort.base.metric.SourceMetricData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.inlong.sort.base.Constants.NUM_BYTES_IN;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_IN;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_OUT;

/**
 * metric state for {@link MetricState} supporting snapshot and restore
 */
public class MetricStateUtils {

    public static final Logger LOGGER = LoggerFactory.getLogger(MetricStateUtils.class);

    /**
     *
     * restore metric state data
     * @param metricStateListState state data list
     * @param subtaskIndex current subtask index
     * @param currentSubtaskNum number of current parallel subtask
     * @return metric state
     * @throws Exception throw exception metricStateListState.get()
     */
    public static MetricState restoreMetricState(ListState<MetricState> metricStateListState, Integer subtaskIndex,
            Integer currentSubtaskNum) throws Exception {
        if (metricStateListState == null || metricStateListState.get() == null) {
            return null;
        }
        LOGGER.info("restoreMetricState:{}, subtaskIndex:{}, currentSubtaskNum:{}", metricStateListState, subtaskIndex,
                currentSubtaskNum);
        MetricState currentMetricState;
        Map<Integer, MetricState> map = new HashMap<>(16);
        for (MetricState metricState : metricStateListState.get()) {
            map.put(metricState.getSubtaskIndex(), metricState);
        }
        int previousSubtaskNum = map.size();
        if (currentSubtaskNum >= previousSubtaskNum) {
            currentMetricState = map.get(subtaskIndex);
        } else {
            Map<String, Long> metrics = new HashMap<>(4);
            currentMetricState = new MetricState(subtaskIndex, metrics);
            List<Integer> indexList = computeIndexList(subtaskIndex, currentSubtaskNum, previousSubtaskNum);
            for (Integer index : indexList) {
                MetricState metricState = map.get(index);
                for (Map.Entry<String, Long> entry : metricState.getMetrics().entrySet()) {
                    if (metrics.containsKey(entry.getKey())) {
                        metrics.put(entry.getKey(), metrics.get(entry.getKey()) + entry.getValue());
                    } else {
                        metrics.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
        return currentMetricState;
    }

    /**
     *
     * Assignment previous subtask index to current subtask when reduce parallelism
     * n = N/m, get n old task per new subtask, mth new subtask get (N - (m - 1) * n) old task
     * @param subtaskIndex current subtask index
     * @param currentSubtaskNum number of current parallel subtask
     * @param previousSubtaskNum number of previous parallel subtask
     * @return index list
     */
    public static List<Integer> computeIndexList(Integer subtaskIndex, Integer currentSubtaskNum,
            Integer previousSubtaskNum) {
        List<Integer> indexList = new ArrayList<>();
        int assignTaskNum = previousSubtaskNum / currentSubtaskNum;
        if (subtaskIndex == currentSubtaskNum - 1) {
            for (int i = subtaskIndex * assignTaskNum; i < previousSubtaskNum; i++) {
                indexList.add(i);
            }
        } else {
            for (int i = 1; i <= assignTaskNum; i++) {
                indexList.add(i + subtaskIndex * assignTaskNum - 1);
            }
        }
        return indexList;
    }

    /**
     *
     * Snapshot metric state data for {@link SourceMetricData}
     * @param metricStateListState state data list
     * @param sourceMetricData {@link SourceMetricData} A collection class for handling metrics
     * @param subtaskIndex subtask index
     * @throws Exception throw exception when add metric state
     */
    public static void snapshotMetricStateForSourceMetricData(ListState<MetricState> metricStateListState,
            SourceMetricData sourceMetricData, Integer subtaskIndex)
            throws Exception {
        LOGGER.info("snapshotMetricStateForSourceMetricData:{}, sourceMetricData:{}, subtaskIndex:{}",
                metricStateListState, sourceMetricData, subtaskIndex);
        metricStateListState.clear();
        Map<String, Long> metricDataMap = new HashMap<>();
        metricDataMap.put(NUM_RECORDS_IN, sourceMetricData.getNumRecordsIn().getCount());
        metricDataMap.put(NUM_BYTES_IN, sourceMetricData.getNumBytesIn().getCount());
        MetricState metricState = new MetricState(subtaskIndex, metricDataMap);
        metricStateListState.add(metricState);
    }

    /**
     *
     * Snapshot metric state data for {@link SinkMetricData}
     * @param metricStateListState state data list
     * @param sinkMetricData {@link SinkMetricData} A collection class for handling metrics
     * @param subtaskIndex subtask index
     * @throws Exception throw exception when add metric state
     */
    public static void snapshotMetricStateForSinkMetricData(ListState<MetricState> metricStateListState,
            SinkMetricData sinkMetricData, Integer subtaskIndex)
            throws Exception {
        LOGGER.info("snapshotMetricStateForSinkMetricData:{}, sinkMetricData:{}, subtaskIndex:{}",
                metricStateListState, sinkMetricData, subtaskIndex);
        metricStateListState.clear();
        Map<String, Long> metricDataMap = new HashMap<>();
        metricDataMap.put(NUM_RECORDS_OUT, sinkMetricData.getNumRecordsOut().getCount());
        metricDataMap.put(NUM_BYTES_OUT, sinkMetricData.getNumBytesOut().getCount());
        MetricState metricState = new MetricState(subtaskIndex, metricDataMap);
        metricStateListState.add(metricState);
    }

}
