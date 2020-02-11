/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tubemq.server.broker.msgstore.ssd;

/***
 * Transfer files on disk to ssd event.
 */
public class SSDSegEvent {
    public final String storeKey;
    public final String partStr;
    public final long startOffset;
    public final int segCnt;


    public SSDSegEvent(final String partStr, final String storeKey,
                       final long startOffset, final int segCnt) {
        this.partStr = partStr;
        this.storeKey = storeKey;
        this.startOffset = startOffset;
        this.segCnt = segCnt;
    }

    @Override
    public String toString() {
        return new StringBuilder(512).append("{storeKey=").append(storeKey)
                .append(",partStr=").append(partStr).append(",startOffset=")
                .append(startOffset).append(",segCnt=").append(segCnt)
                .append("}").toString();
    }


}
