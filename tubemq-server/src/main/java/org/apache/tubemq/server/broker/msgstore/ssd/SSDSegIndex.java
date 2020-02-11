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

/**
 * SSD segment index.
 */
public class SSDSegIndex {
    public final String storeKey;
    public final long startOffset;
    public final long endOffset;

    public SSDSegIndex(final String storeKey,
                       final long startOffset,
                       final long endOffset) {
        this.storeKey = storeKey;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SSDSegIndex)) {
            return false;
        }

        SSDSegIndex that = (SSDSegIndex) o;
        if (startOffset != that.startOffset) {
            return false;
        }
        if (endOffset != that.endOffset) {
            return false;
        }
        return storeKey.equals(that.storeKey);

    }

    @Override
    public int hashCode() {
        int result = storeKey.hashCode();
        result = 31 * result + (int) (startOffset ^ (startOffset >>> 32));
        result = 31 * result + (int) (endOffset ^ (endOffset >>> 32));
        return result;
    }

}
