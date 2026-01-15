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

import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.IgnoreFailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.NoOpFailureHandler;
import org.apache.inlong.sort.formats.metrics.FormatMetricGroup;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The base implementation of {@link DeserializationSchema}.
 */
public abstract class DefaultDeserializationSchema<T> implements DeserializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDeserializationSchema.class);

    protected long lastPrintTimestamp = 0L;
    protected long PRINT_TIMESTAMP_INTERVAL = 60 * 1000L;
    protected int fieldNameSize = 0;

    protected FailureHandler failureHandler;

    /**
     * The format metric group.
     */
    protected transient FormatMetricGroup formatMetricGroup;

    public DefaultDeserializationSchema(boolean ignoreErrors) {
        if (ignoreErrors) {
            failureHandler = new IgnoreFailureHandler();
        } else {
            failureHandler = new NoOpFailureHandler();
        }
    }

    public DefaultDeserializationSchema(FailureHandler failureHandler) {
        this.failureHandler = failureHandler;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
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
     * Deserialize the data and handle the failure.
     *
     * <p>Note: Returns null if the message could not be properly deserialized.
     */
    @Override
    public T deserialize(byte[] bytes) throws IOException {
        try {
            T result = deserializeInternal(bytes);
            // reset error state after deserialize success
            return result;
        } catch (Exception e) {
            if (formatMetricGroup != null) {
                formatMetricGroup.getNumRecordsDeserializeError().inc(1L);
            }
            if (failureHandler != null && failureHandler.isIgnoreFailure()) {
                if (formatMetricGroup != null) {
                    formatMetricGroup.getNumRecordsDeserializeErrorIgnored().inc(1L);
                }
                if (bytes == null) {
                    LOG.warn("Could not properly deserialize the data null.");
                } else {
                    LOG.warn("Could not properly deserialize the data {}.",
                            StringUtils.byteToHexString(bytes), e);
                }
                return null;
            }
            throw new IOException("Failed to deserialize data " +
                    StringUtils.byteToHexString(bytes), e);
        }
    }

    protected boolean needPrint() {
        long now = Instant.now().toEpochMilli();
        if (now - lastPrintTimestamp > PRINT_TIMESTAMP_INTERVAL) {
            lastPrintTimestamp = now;
            return true;
        }
        return false;
    }

    protected abstract T deserializeInternal(byte[] bytes) throws Exception;

    public abstract FormatMsg deserializeFormatMsg(byte[] bytes) throws Exception;

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        DefaultDeserializationSchema<?> that = (DefaultDeserializationSchema<?>) object;
        return Objects.equals(failureHandler, that.failureHandler);
    }

    protected void checkFieldNameSize(String body, int actualNumFields, int fieldNameSize,
            FailureHandler failureHandler) {
        if (actualNumFields != fieldNameSize) {
            if (failureHandler != null) {
                failureHandler.onFieldNumError(null, null, body,
                        actualNumFields, fieldNameSize);
            } else {
                LOG.warn("The number of fields mismatches: {}"
                        + ",expected, but was {}. origin text: {}",
                        fieldNameSize, actualNumFields, body);
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(failureHandler);
    }
}
