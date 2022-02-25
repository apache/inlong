/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.singletenant.flink.deserialization;

import org.apache.flink.types.Row;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.util.DefaultValueStrategy;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkNotNull;

public class FieldMappingTransformer {

    /**
     * Skips time and attribute fields of source record.
     */
    public static final int SOURCE_FIELD_SKIP_STEP = 0;

    private final FieldInfo[] outputFieldInfos;

    /**
     * Most of the data flows would not config any default value behavior.
     */
    private final DefaultValueStrategy defaultValueStrategy;

    public FieldMappingTransformer(Configuration config, FieldInfo[] outputFieldInfos) {
        this.defaultValueStrategy = new DefaultValueStrategy(checkNotNull(config));
        this.outputFieldInfos = checkNotNull(outputFieldInfos);
    }

    public Row transform(Row sourceRow, long dt) {
        final Row outputRow = new Row(outputFieldInfos.length);
        int sourceRowIndex = SOURCE_FIELD_SKIP_STEP;
        for (int i = 0; i < outputFieldInfos.length; i++) {
            Object fieldValue = null;
            if (outputFieldInfos[i] instanceof BuiltInFieldInfo) {
                BuiltInFieldInfo builtInFieldInfo = (BuiltInFieldInfo) outputFieldInfos[i];
                fieldValue = transformBuiltInField(builtInFieldInfo, dt);
            } else if (sourceRowIndex < sourceRow.getArity()) {
                fieldValue = sourceRow.getField(sourceRowIndex);
                sourceRowIndex++;
            }

            if (fieldValue == null) {
                fieldValue = defaultValueStrategy.getDefaultValue(outputFieldInfos[i].getFormatInfo());
            }
            outputRow.setField(i, fieldValue);
        }
        return outputRow;
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
}
