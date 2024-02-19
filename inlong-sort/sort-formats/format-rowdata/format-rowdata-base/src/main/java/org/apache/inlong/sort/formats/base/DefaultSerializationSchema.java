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

package org.apache.inlong.sort.formats.base;

import org.apache.inlong.sort.formats.metrics.FormatMetricGroup;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The base implementation of {@link SerializationSchema}.
 */
public abstract class DefaultSerializationSchema<T> implements SerializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSerializationSchema.class);

    /**
     * If true, the serialization error will be ignored.
     */
    private final boolean ignoreErrors;

    /**
     * If true, a parsing error is occurred.
     */
    private boolean errorOccurred = false;

    /**
     * The format metric group.
     */
    protected transient FormatMetricGroup formatMetricGroup;

    public DefaultSerializationSchema(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) {
        try {
            MetricGroup metricGroup = context.getMetricGroup();

            checkArgument(metricGroup instanceof FormatMetricGroup,
                    "Expecting FormatMetricGroup, but got " + metricGroup.getClass().getName());

            this.formatMetricGroup = (FormatMetricGroup) metricGroup;
        } catch (Exception ignore) {
            LOG.warn("FormatGroup initialization error, no format metric will be accumulated", ignore);
        }
    }

    /**
     * Serialize the data and handle the failure.
     *
     * <p>Note: Returns null if the message could not be properly serialized.
     */
    @Override
    public byte[] serialize(T data) {
        try {
            byte[] result = serializeInternal(data);
            // reset error state after serialize success
            errorOccurred = false;
            return result;
        } catch (Exception e) {
            errorOccurred = true;
            if (formatMetricGroup != null) {
                formatMetricGroup.getNumRecordsSerializeError().inc(1L);
            }
            if (ignoreErrors) {
                if (formatMetricGroup != null) {
                    formatMetricGroup.getNumRecordsSerializeErrorIgnored().inc(1L);
                }
                LOG.warn("Could not properly serialize the data {}", data, e);
                return null;
            }
            throw new RuntimeException("Failed to serialize data " + data, e);
        }
    }

    public boolean skipCurrentRecord(T element) {
        return ignoreErrors && errorOccurred;
    }

    protected abstract byte[] serializeInternal(T data) throws IOException;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultSerializationSchema<?> that = (DefaultSerializationSchema<?>) o;
        return Objects.equals(ignoreErrors, that.ignoreErrors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ignoreErrors);
    }
}
