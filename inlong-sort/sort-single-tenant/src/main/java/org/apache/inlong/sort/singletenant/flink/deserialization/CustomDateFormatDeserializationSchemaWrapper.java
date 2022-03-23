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

package org.apache.inlong.sort.singletenant.flink.deserialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.guava18.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.formats.common.DateFormatInfo;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;

import java.io.IOException;
import java.text.ParseException;

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.isStandardTimestampFormat;

public class CustomDateFormatDeserializationSchemaWrapper implements DeserializationSchema<Row> {

    private static final long serialVersionUID = -8124501384364175884L;

    private final DeserializationSchema<Row> innerSchema;

    private final FormatInfo[] formatInfos;

    public CustomDateFormatDeserializationSchemaWrapper(
            DeserializationSchema<Row> innerSchema, FormatInfo[] formatInfos) {
        this.innerSchema = checkNotNull(innerSchema);
        this.formatInfos = checkNotNull(formatInfos);
    }

    @Override
    public Row deserialize(byte[] message) {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<Row> out) throws IOException {
        ListCollector<Row> collector = new ListCollector<>();
        innerSchema.deserialize(message, collector);
        for (Row row : collector.getInnerList()) {
            try {
                out.collect(fromStringToDateAndTime(row));
            } catch (ParseException e) {
                throw new IOException("Failed to parse input date or time, error is " + e);
            }
        }
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return innerSchema.isEndOfStream(nextElement);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return innerSchema.getProducedType();
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        innerSchema.open(context);
    }

    @VisibleForTesting
    Row fromStringToDateAndTime(Row inputRow) throws ParseException {
        int arity = inputRow.getArity();
        Row outputRow = new Row(arity);
        for (int i = 0; i < arity; i++) {
            outputRow.setField(i, convert(inputRow.getField(i), formatInfos[i]));
        }
        outputRow.setKind(inputRow.getKind());
        return outputRow;
    }

    private Object convert(Object input, FormatInfo formatInfo) throws ParseException {
        if (input == null) {
            return null;
        }

        if (formatInfo instanceof DateFormatInfo && !isStandardTimestampFormat(formatInfo)) {
            return ((DateFormatInfo) formatInfo).deserialize((String) input);

        } else if (formatInfo instanceof TimeFormatInfo && !isStandardTimestampFormat(formatInfo)) {
            return ((TimeFormatInfo) formatInfo).deserialize((String) input);

        } else if (formatInfo instanceof TimestampFormatInfo && !isStandardTimestampFormat(formatInfo)) {
            return ((TimestampFormatInfo) formatInfo).deserialize((String) input);

        } else {
            return input;
        }
    }

}
