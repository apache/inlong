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

/**
 * Audit cycle
 */
public enum AuditCycle {

    MINUTE(1), MINUTE_5(5), MINUTE_10(10), MINUTE_30(30), HOUR(60), DAY(1440), UNKNOWN(1000);

    private final int cycle;

    AuditCycle(int cycle) {
        this.cycle = cycle;
    }

    public int getValue() {
        return cycle;
    }

    /**
     * Convert int to AuditCycle.
     *
     * @param value
     * @return
     */
    public static AuditCycle fromInt(int value) {
        for (AuditCycle auditCycle : AuditCycle.values()) {
            if (auditCycle.getValue() == value) {
                return auditCycle;
            }
        }
        return UNKNOWN;
    }
}
