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
package org.apache.tubemq.client.consumer;

public enum ConsumeModel {
    CONSUMER_FROM_MAX_OFFSET(1, "Always start from the max consume position."),
    CONSUMER_FROM_LATEST_OFFSET(0, "Start from the latest position for the first time. " +
            "Otherwise start from last consume position."),
    CONSUMER_FROM_FIRST_OFFSET(-1, "Start from 0 for the first time. Otherwise start from last consume position.");

    private int code;
    private String description;

    ConsumeModel(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}
