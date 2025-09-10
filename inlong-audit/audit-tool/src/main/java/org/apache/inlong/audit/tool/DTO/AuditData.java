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

package org.apache.inlong.audit.tool.DTO;

import lombok.Data;

@Data
public class AuditData {

    private int id;
    private String ip;
    private String dockerId;
    private String threadId;
    private long sdkTs;
    private long packetId;
    private long logTs;
    private String groupId;
    private String streamId;
    private String auditId;
    private String auditTag;
    private long auditVersion;
    private long count;
    private long size;
    private long delay;
    private long updateTime;

    public double getSize() {
        return size;
    }

    public double getDataLossRate() {

        return count > 0 ? (double) (count - size) / count : 0.0;
    }

    public long getDataLossCount() {
        return count - size;
    }

    public long getAuditCount() {
        return count;
    }

    public long getExpectedCount() {

        return count;
    }

    public long getReceivedCount() {

        return size;
    }
}