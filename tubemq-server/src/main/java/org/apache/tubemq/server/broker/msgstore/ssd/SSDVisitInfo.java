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

package org.apache.tubemq.server.broker.msgstore.ssd;

/***
 * SSD visit record.
 */
public class SSDVisitInfo {
    public final String partStr;
    public long lastOffset;
    public long lastTime;

    public SSDVisitInfo(final String partStr, final long lastOffset) {
        this.partStr = partStr;
        this.lastOffset = lastOffset;
        this.lastTime = System.currentTimeMillis();
    }

    public void requestVisit(final long lastOffset) {
        this.lastOffset = lastOffset;
        this.lastTime = System.currentTimeMillis();
    }

    public void responseVisit(final long lastOffset) {
        this.lastOffset = lastOffset;
        this.lastTime = System.currentTimeMillis();
    }

}
