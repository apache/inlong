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

package org.apache.inlong.sort.singletenant.flink.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.guava18.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.common.DateFormatInfo;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.isStandardTimestampFormat;

public class CustomDateFormatSerializationSchemaWrapper implements SerializationSchema<Row> {

    private static final long serialVersionUID = 7342088340154604198L;

    private final SerializationSchema<Row> innerSchema;

    private final FormatInfo[] formatInfos;

    public CustomDateFormatSerializationSchemaWrapper(SerializationSchema<Row> innerSchema, FormatInfo[] formatInfos) {
        this.innerSchema = checkNotNull(innerSchema);
        this.formatInfos = checkNotNull(formatInfos);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        innerSchema.open(context);
    }

    @Override
    public byte[] serialize(Row element) {
        Row outputRow = fromDateAndTimeToString(element);
        return innerSchema.serialize(outputRow);
    }

    @VisibleForTesting
    Row fromDateAndTimeToString(Row inputRow) {
        int arity = inputRow.getArity();
        Row outputRow = new Row(arity);
        for (int i = 0; i < arity; i++) {
            outputRow.setField(i, convert(inputRow.getField(i), formatInfos[i]));
        }
        outputRow.setKind(inputRow.getKind());
        return outputRow;
    }

    private Object convert(Object input, FormatInfo formatInfo) {
        if (input == null) {
            return null;
        }

        if (formatInfo instanceof DateFormatInfo && !isStandardTimestampFormat(formatInfo)) {
            return ((DateFormatInfo) formatInfo).serialize((Date) input);

        } else if (formatInfo instanceof TimeFormatInfo && !isStandardTimestampFormat(formatInfo)) {
            return ((TimeFormatInfo) formatInfo).serialize((Time) input);

        } else if (formatInfo instanceof TimestampFormatInfo && !isStandardTimestampFormat(formatInfo)) {
            return ((TimestampFormatInfo) formatInfo).serialize((Timestamp) input);

        } else {
            return input;
        }
    }
}
