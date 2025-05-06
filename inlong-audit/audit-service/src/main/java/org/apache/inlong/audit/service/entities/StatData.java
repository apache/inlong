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

package org.apache.inlong.audit.service.entities;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
public class StatData {

    private long auditVersion;
    private String logTs;
    private String inlongGroupId;
    private String inlongStreamId;
    private String auditId;
    private String auditTag;
    private Long count = 0L;
    private Long size = 0L;
    private Long delay = 0L;
    private Timestamp updateTime;
    private String ip;
    private String sourceName;

    /**
     * Add values to statistics fields with null checks
     * @param count message count to add
     * @param size data size to add
     * @param delay delay time to add
     */
    public void add(Long count, Long size, Long delay) {
        if (count != null) {
            this.count += count;
        }
        if (size != null) {
            this.size += size;
        }
        if (delay != null) {
            this.delay += delay;
        }
    }

    /**
     * Build composite key with format "logTs|inlongGroupId|auditId"
     * @return composite key string
     */
    public String getCompositeKey() {
        return String.join("|", logTs, inlongGroupId, auditId);
    }

    /**
     * Constructor with essential fields
     * @param logTs timestamp string
     * @param inlongGroupId group ID
     * @param inlongStreamId stream ID
     * @param auditId audit ID
     */
    public StatData(String logTs, String inlongGroupId, String inlongStreamId, String auditId) {
        this.logTs = logTs;
        this.inlongGroupId = inlongGroupId;
        this.inlongStreamId = inlongStreamId;
        this.auditId = auditId;
    }
}
