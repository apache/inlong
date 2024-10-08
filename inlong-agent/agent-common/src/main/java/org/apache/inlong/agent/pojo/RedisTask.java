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

package org.apache.inlong.agent.pojo;

import lombok.Data;

@Data
public class RedisTask {

    private String authUser;
    private String authPassword;
    private String hostname;
    private String port;
    private Boolean ssl;
    private String readTimeout;
    private String queueSize;
    private String replId;
    private String dbName;
    private String command;
    private String keys;
    private String fieldOrMember;
    private Boolean isSubscribe;
    private String syncFreq;
    private String subscriptionOperation;

    @Data
    public static class RedisTaskConfig {

        private String username;
        private String password;
        private String hostname;
        private String port;
        private Boolean ssl;
        private String timeout;
        private String queueSize;
        private String replId;
        private String dbName;
        private String command;
        private String keys;
        private String fieldOrMember;
        private Boolean isSubscribe;
        private String syncFreq;
        private String subscriptionOperation;
    }
}
