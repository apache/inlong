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

package org.apache.inlong.sort.formats.inlongmsg.row;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The base for all InLongMsg mixed format converters.
 */
public abstract class AbstractInLongMsgMixedFormatConverter
        implements
            InLongMsgMixedFormatConverter {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractInLongMsgMixedFormatConverter.class);

    /**
     * True if ignore errors in the deserialization.
     */
    private final boolean ignoreErrors;

    public AbstractInLongMsgMixedFormatConverter(
            boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
    }

    public abstract List<Row> convertRows(
            Map<String, String> attributes,
            byte[] data,
            String streamId,
            Timestamp time,
            List<String> predefinedFields,
            List<String> fields,
            Map<String, String> entries) throws Exception;

    @Override
    public void flatMap(Row row, Collector<Row> collector) throws Exception {

        List<Row> convertedRows;

        try {
            Map<String, String> attributes = InLongMsgUtils.getAttributesFromMixedRow(row);
            byte[] data = InLongMsgUtils.getDataFromMixedRow(row);
            String streamId = InLongMsgUtils.getStreamIdFromMixedRow(row);
            Timestamp time = InLongMsgUtils.getTimeFromMixedRow(row);
            List<String> predefinedFields = InLongMsgUtils.getPredefinedFieldsFromMixedRow(row);
            List<String> fields = InLongMsgUtils.getFieldsFromMixedRow(row);
            Map<String, String> entries = InLongMsgUtils.getEntriesFromMixedRow(row);

            convertedRows =
                    convertRows(attributes, data, streamId, time, predefinedFields, fields, entries);
        } catch (Throwable t) {
            String errorMessage =
                    String.format("Could not properly convert the mixed row. Row=[%s].", row);
            if (ignoreErrors) {
                LOG.warn(errorMessage, t);
                return;
            } else {
                throw new RuntimeException(errorMessage, t);
            }
        }

        if (convertedRows != null) {
            for (Row convertedRow : convertedRows) {
                collector.collect(convertedRow);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractInLongMsgMixedFormatConverter that = (AbstractInLongMsgMixedFormatConverter) o;
        return Objects.equals(ignoreErrors, that.ignoreErrors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ignoreErrors);
    }

    /**
     * The context to create instance of {@link AbstractInLongMsgMixedFormatConverter}.
     */
    public interface TableFormatContext {

        MetricGroup getMetricGroup();

        Map<String, String> getFormatProperties();
    }
}
