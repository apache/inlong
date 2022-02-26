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

package org.apache.inlong.sort.flink.transformation;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.util.CommonUtils;
import org.apache.inlong.sort.util.DefaultValueStrategy;

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkNotNull;

/**
 * TODO, replace it with operator when it supports complex transformation.
 */
public class FieldMappingTransformer implements DataFlowInfoListener {

    /**
     * Skips time and attribute fields of source record.
     *
     * TODO, are all formats should be skipped by 2?
     */
    public static final int SOURCE_FIELD_SKIP_STEP = 2;

    private final Map<Long, FieldInfo[]> sinkFieldInfoMap = new HashMap<>();

    private final Map<Long, DefaultValueStrategy> defaultValueStrategies = new HashMap<>();

    private final Configuration config;

    /**
     * Most of the data flows would not config any default value behavior.
     */
    private final DefaultValueStrategy fallbackDefaultValueStrategy;

    public FieldMappingTransformer(Configuration config) {
        this.config = checkNotNull(config);
        this.fallbackDefaultValueStrategy = new DefaultValueStrategy(config);
    }

    @Override
    public void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
        updateDataFlow(dataFlowInfo);
    }

    public void updateDataFlow(DataFlowInfo dataFlowInfo) {
        sinkFieldInfoMap.put(dataFlowInfo.getId(), dataFlowInfo.getSourceInfo().getFields());
        final Configuration dataFlowConfig = CommonUtils.mergeConfAndProp(config, dataFlowInfo.getProperties());
        final DefaultValueStrategy defaultValueStrategy = new DefaultValueStrategy(dataFlowConfig);
        if (!defaultValueStrategy.equals(fallbackDefaultValueStrategy)) {
            defaultValueStrategies.put(dataFlowInfo.getId(), defaultValueStrategy);
        }
    }

    public void removeDataFlow(DataFlowInfo dataFlowInfo) {
        sinkFieldInfoMap.remove(dataFlowInfo.getId());
        defaultValueStrategies.remove(dataFlowInfo.getId());
    }

    public Record transform(Record sourceRecord) throws Exception {
        final FieldInfo[] sinkFieldInfos = sinkFieldInfoMap.get(sourceRecord.getDataflowId());
        if (sinkFieldInfos == null) {
            throw new Exception("No field info found for data flow:" + sourceRecord.getDataflowId());
        }
        final Row sourceRow = sourceRecord.getRow();
        final Row sinkRow = new Row(sinkFieldInfos.length);
        int fieldIndex = SOURCE_FIELD_SKIP_STEP;
        for (int i = 0; i < sinkFieldInfos.length; i++) {
            Object fieldValue = null;
            if (sinkFieldInfos[i] instanceof BuiltInFieldInfo) {
                BuiltInFieldInfo builtInFieldInfo = (BuiltInFieldInfo) sinkFieldInfos[i];
                fieldValue = transformBuiltInField(
                        builtInFieldInfo,
                        sourceRecord.getTimestampMillis()
                );
            } else if (fieldIndex < sourceRow.getArity()) {
                fieldValue = sourceRow.getField(fieldIndex);
                fieldIndex++;
            }

            if (fieldValue == null) {
                fieldValue = getDefaultValue(sourceRecord.getDataflowId(), sinkFieldInfos[i].getFormatInfo());
            }
            sinkRow.setField(i, fieldValue);
        }
        return new Record(sourceRecord.getDataflowId(), sourceRecord.getTimestampMillis(), sinkRow);
    }

    private static Object transformBuiltInField(BuiltInFieldInfo builtInFieldInfo, long dataTimestamp) {
        if (builtInFieldInfo.getBuiltInField() == BuiltInFieldInfo.BuiltInField.DATA_TIME) {
            return inferDataTimeValue(builtInFieldInfo.getFormatInfo(), dataTimestamp);
        }
        return null;
    }

    /**
     * Infers the field value of data time from format info. Timestamp, Time, Date are acceptable.
     *
     * @param dataTimeFormatInfo the format info user specified
     * @param dataTime           original data time in long format
     * @return the inferred field value
     */
    private static Date inferDataTimeValue(FormatInfo dataTimeFormatInfo, long dataTime) {
        final Date dataTimeValue;
        if (dataTimeFormatInfo instanceof TimestampFormatInfo) {
            dataTimeValue = new Timestamp(dataTime);
        } else if (dataTimeFormatInfo instanceof TimeFormatInfo) {
            dataTimeValue = new Time(dataTime);
        } else {
            dataTimeValue = new Date(dataTime);
        }
        return dataTimeValue;
    }

    private Object getDefaultValue(Long dataFlowId, FormatInfo formatInfo) {
        final DefaultValueStrategy strategy =
                defaultValueStrategies.getOrDefault(dataFlowId, fallbackDefaultValueStrategy);
        return strategy.getDefaultValue(formatInfo);
    }
}
