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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Copied from Apache Flink project {@link org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner}.
 */
public interface BucketAssigner<IN, BucketID> extends Serializable {

    /**
     * Returns the identifier of the bucket the provided element should be put into.
     *
     * @param element The current element being processed.
     * @param context The {@link SinkFunction.Context context} used by the {@link StreamingFileSink sink}.
     * @return A string representing the identifier of the bucket the element should be put into.
     *         The actual path to the bucket will result from the concatenation of the returned string
     *         and the {@code base path} provided during the initialization of the {@link StreamingFileSink sink}.
     */
    BucketID getBucketId(IN element, BucketAssigner.Context context);

    /**
     * @return A {@link SimpleVersionedSerializer} capable of serializing/deserializing the elements
     *         of type {@code BucketID}. That is the type of the objects returned by the
     *         {@link #getBucketId(Object, BucketAssigner.Context)}.
     */
    SimpleVersionedSerializer<BucketID> getSerializer();

    /**
     * Context that the {@link BucketAssigner} can use for getting additional data about
     * an input record.
     *
     * <p>The context is only valid for the duration of a {@link BucketAssigner#getBucketId(Object,
     * BucketAssigner.Context)} call.
     * Do not store the context and use afterwards!
     */
    interface Context {

        /**
         * Returns the current processing time.
         */
        long currentProcessingTime();

        /**
         * Returns the current event-time watermark.
         */
        long currentWatermark();

        /**
         * Returns the timestamp of the current input record or
         * {@code null} if the element does not have an assigned timestamp.
         */
        @Nullable
        Long timestamp();
    }
}
