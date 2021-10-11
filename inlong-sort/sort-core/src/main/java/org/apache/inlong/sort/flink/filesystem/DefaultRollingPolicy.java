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

package org.apache.inlong.sort.flink.filesystem;

import com.google.common.base.Preconditions;
import java.io.IOException;

/**
 * Copied from Apache Flink project,
 * {@link org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy}.
 */
public final class DefaultRollingPolicy<IN, BucketID> implements RollingPolicy<IN, BucketID> {

    private static final long serialVersionUID = 1L;

    private static final long DEFAULT_INACTIVITY_INTERVAL = 60L * 1000L;

    private static final long DEFAULT_ROLLOVER_INTERVAL = 60L * 1000L;

    private static final long DEFAULT_MAX_PART_SIZE = 1024L * 1024L * 128L;

    private final long partSize;

    private final long rolloverInterval;

    private final long inactivityInterval;

    /**
     * Private constructor to avoid direct instantiation.
     */
    private DefaultRollingPolicy(long partSize, long rolloverInterval, long inactivityInterval) {
        Preconditions.checkArgument(partSize > 0L);
        Preconditions.checkArgument(rolloverInterval > 0L);
        Preconditions.checkArgument(inactivityInterval > 0L);

        this.partSize = partSize;
        this.rolloverInterval = rolloverInterval;
        this.inactivityInterval = inactivityInterval;
    }

    @Override
    public boolean shouldRollOnCheckpoint(PartFileInfo<BucketID> partFileState) throws IOException {
        return true;
    }

    @Override
    public boolean shouldRollOnEvent(PartFileInfo<BucketID> partFileState, IN element) throws IOException {
        return partFileState.getSize() > partSize;
    }

    @Override
    public boolean shouldRollOnProcessingTime(final PartFileInfo<BucketID> partFileState, final long currentTime) {
        return currentTime - partFileState.getCreationTime() >= rolloverInterval
                || currentTime - partFileState.getLastUpdateTime() >= inactivityInterval;
    }

    /**
     * Initiates the instantiation of a {@code DefaultRollingPolicy}.
     * To finalize it and have the actual policy, call {@code .create()}.
     */
    public static DefaultRollingPolicy.PolicyBuilder create() {
        return new DefaultRollingPolicy.PolicyBuilder(
                DEFAULT_MAX_PART_SIZE,
                DEFAULT_ROLLOVER_INTERVAL,
                DEFAULT_INACTIVITY_INTERVAL);
    }

    /**
     * A helper class that holds the configuration properties for the {@link DefaultRollingPolicy}.
     */
    public static final class PolicyBuilder {

        private final long partSize;

        private final long rolloverInterval;

        private final long inactivityInterval;

        private PolicyBuilder(
                final long partSize,
                final long rolloverInterval,
                final long inactivityInterval) {
            this.partSize = partSize;
            this.rolloverInterval = rolloverInterval;
            this.inactivityInterval = inactivityInterval;
        }

        /**
         * Sets the part size above which a part file will have to roll.
         *
         * @param size the allowed part size.
         */
        public DefaultRollingPolicy.PolicyBuilder withMaxPartSize(final long size) {
            Preconditions.checkState(size > 0L);
            return new DefaultRollingPolicy.PolicyBuilder(size, rolloverInterval, inactivityInterval);
        }

        /**
         * Sets the interval of allowed inactivity after which a part file will have to roll.
         *
         * @param interval the allowed inactivity interval.
         */
        public DefaultRollingPolicy.PolicyBuilder withInactivityInterval(final long interval) {
            Preconditions.checkState(interval > 0L);
            return new DefaultRollingPolicy.PolicyBuilder(partSize, rolloverInterval, interval);
        }

        /**
         * Sets the max time a part file can stay open before having to roll.
         *
         * @param interval the desired rollover interval.
         */
        public DefaultRollingPolicy.PolicyBuilder withRolloverInterval(final long interval) {
            Preconditions.checkState(interval > 0L);
            return new DefaultRollingPolicy.PolicyBuilder(partSize, interval, inactivityInterval);
        }

        /**
         * Creates the actual policy.
         */
        public <I, B> DefaultRollingPolicy<I, B> build() {
            return new DefaultRollingPolicy<>(partSize, rolloverInterval, inactivityInterval);
        }
    }
}
