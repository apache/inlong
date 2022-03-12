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

import java.io.IOException;

/**
 * Copied from Apache Flink project, {@link org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo}.
 */
public interface PartFileInfo<BucketID> {

    /**
     * The bucket identifier of the current buffer
     * @return The bucket identifier of the current buffer, as returned by the
     * {@link BucketAssigner#getBucketId(Object, BucketAssigner.Context)}.
     */
    BucketID getBucketId();

    /**
     *  The creation time (in ms) of the currently open part file.
     */
    long getCreationTime();

    /**
     *  The size of the currently open part file.
     */
    long getSize() throws IOException;

    /**
     *  The last time (in ms) the currently open part file was written to.
     */
    long getLastUpdateTime();
}
