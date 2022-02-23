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

package org.apache.inlong.common.metrics;

public class ChannelMetric {

    private long currentBufferSize;
    private float middleWaterMarkSize;
    private boolean isOverHighWaterMark;
    private boolean isOverMiddleWaterMark;

    public boolean isOverHighWaterMark() {
        return isOverHighWaterMark;
    }

    public void setOverHighWaterMark(boolean overHighWaterMark) {
        isOverHighWaterMark = overHighWaterMark;
    }

    public boolean isOverMiddleWaterMark() {
        return isOverMiddleWaterMark;
    }

    public void setOverMiddleWaterMark(boolean overMiddleWaterMark) {
        isOverMiddleWaterMark = overMiddleWaterMark;
    }

    public long getCurrentBufferSize() {
        return currentBufferSize;
    }

    public void addCurrentBufferSize(long byteNum) {
        this.currentBufferSize += byteNum;
    }

    public float getMiddleWaterMarkSize() {
        return middleWaterMarkSize;
    }

    public void setMiddleWaterMarkSize(float middleWaterMarkSize) {
        this.middleWaterMarkSize = middleWaterMarkSize;
    }
}
