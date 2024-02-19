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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The base implementation of {@link DeserializationSchema}.
 */
public abstract class DefaultDeserializationSchema<T> implements DeserializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDeserializationSchema.class);

    /**
     * If true, the deserialization error will be ignored.
     */
    protected final boolean ignoreErrors;

    /**
     * If true, a parsing error is occurred.
     */
    private boolean errorOccurred = false;

    /**
     * The metric counting the number of ignored errors.
     */
    private transient Counter numIgnoredErrors;

    public DefaultDeserializationSchema(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
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
            errorOccurred = false;
            return result;
        } catch (Exception e) {
            if (ignoreErrors) {
                errorOccurred = true;
                getNumIgnoredErrors().inc();
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

    protected abstract T deserializeInternal(byte[] bytes) throws IOException;

    public boolean skipCurrentRecord(T t) {
        return ignoreErrors && errorOccurred;
    }

    public long numSkippedRecords() {
        return getNumIgnoredErrors().getCount();
    }

    // This method is used to ensure initialization of the 'numIgnoredErrors' field. The field is non-serializable and
    // marked as transient
    private Counter getNumIgnoredErrors() {
        if (numIgnoredErrors == null) {
            numIgnoredErrors = new SimpleCounter();
        }
        return numIgnoredErrors;
    }
}
