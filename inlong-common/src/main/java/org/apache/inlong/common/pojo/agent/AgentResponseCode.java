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

package org.apache.inlong.common.pojo.agent;

public enum AgentResponseCode {

    SUCCESS(0, "SUCCESS", "Get module config success"),
    NO_UPDATE(1, "NO_UPDATE", "No update"),
    UNKNOWN_ERROR(Integer.MAX_VALUE, "UNKNOWN", "Unknown error");

    private final int id;
    private final String name;
    private final String desc;

    AgentResponseCode(int id, String name, String desc) {
        this.id = id;
        this.name = name;
        this.desc = desc;
    }

    public static AgentResponseCode valueOf(int value) {
        for (AgentResponseCode agentResponseCode : AgentResponseCode.values()) {
            if (agentResponseCode.getId() == value) {
                return agentResponseCode;
            }
        }
        return UNKNOWN_ERROR;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDesc() {
        return desc;
    }
}
