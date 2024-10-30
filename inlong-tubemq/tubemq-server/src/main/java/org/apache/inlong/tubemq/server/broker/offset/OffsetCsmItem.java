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

package org.apache.inlong.tubemq.server.broker.offset;

/**
 * The offset snapshot of the consumer group on the partition.
 */
public class OffsetCsmItem {

    protected int partitionId;
    // consume group confirmed offset
    protected long cfmOffset = 0L;
    // consume group fetched offset
    protected long inflightOffset = 0L;
    // consumer clientRecId
    protected int clientRecId = -1;

    public OffsetCsmItem(int partitionId) {
        this.partitionId = partitionId;
    }

    public void addCsmOffsets(long offsetCfm, long offsetFetch) {
        this.cfmOffset = offsetCfm;
        this.inflightOffset = offsetFetch;
    }

    public void addClientRecId(int clientRecId) {
        this.clientRecId = clientRecId;
    }

    public long getCfmOffset() {
        return cfmOffset;
    }
}
