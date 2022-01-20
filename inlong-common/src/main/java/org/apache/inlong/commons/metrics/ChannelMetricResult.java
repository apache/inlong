/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.commons.metrics;

public class ChannelMetricResult {

    private int channelCount;
    private int channelHighWaterMarkCount;
    private int channelMiddleWaterMarkCount;
    private int averageBufferSize;
    private long overAllBufferSize;
    private ResourceUsage memoryUsage;

    public int getChannelCount() {
        return channelCount;
    }

    public void setChannelCount(int channelCount) {
        this.channelCount = channelCount;
    }

    public int getChannelHighWaterMarkCount() {
        return channelHighWaterMarkCount;
    }

    public void setChannelHighWaterMarkCount(int channelHighWaterMarkCount) {
        this.channelHighWaterMarkCount = channelHighWaterMarkCount;
    }

    public int getChannelMiddleWaterMarkCount() {
        return channelMiddleWaterMarkCount;
    }

    public void setChannelMiddleWaterMarkCount(int channelMiddleWaterMarkCount) {
        this.channelMiddleWaterMarkCount = channelMiddleWaterMarkCount;
    }

    public int getAverageBufferSize() {
        return averageBufferSize;
    }

    public void setAverageBufferSize(int averageBufferSize) {
        this.averageBufferSize = averageBufferSize;
    }

    public void setMemoryUsage(ResourceUsage memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    public long getOverAllBufferSize() {
        return overAllBufferSize;
    }

    public void setOverAllBufferSize(long overAllBufferSize) {
        this.overAllBufferSize = overAllBufferSize;
    }
}
