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
import org.apache.inlong.sort.base.metric.SourceMetricData;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.apache.inlong.sort.base.Constants.NUM_BYTES_IN;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_IN;

/**
 * metric data state serialization and deserialization
 */
public class MetricDataSerializationUtils {

    public static Map<String, Long> restoreMetricState(ListState<byte[]> metricState) throws Exception {
        Map<String, Long> metricDataMap = new HashMap<>();
        for (byte[] metricData : metricState.get()) {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(metricData);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            metricDataMap = (Map<String, Long>) objectInputStream.readObject();
        }
        return metricDataMap;
    }

    public static void snapshotMetricState(ListState<byte[]> metricState, SourceMetricData sourceMetricData)
            throws Exception {
        metricState.clear();
        Map<String, Long> metricDataMap = new HashMap<>();
        metricDataMap.put(NUM_RECORDS_IN, sourceMetricData.getNumRecordsIn().getCount());
        metricDataMap.put(NUM_BYTES_IN, sourceMetricData.getNumBytesIn().getCount());
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(metricDataMap);
        metricState.add(byteArrayOutputStream.toByteArray());
    }

}
