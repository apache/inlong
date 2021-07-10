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

package org.apache.inlong.agent.plugin.fetcher.dtos;

import lombok.Data;

@Data
public class JobProfileDto {
    private Job job;
    private Proxy proxy;

    @Data
    public static class Dir {
        private String path;
        private String pattern;
    }

    @Data
    public static class Running {
        private String core;
    }

    @Data
    public static class Thread {
        private Running running;
    }

    @Data
    public static class Job {
        private Dir dir;
        private String trigger;
        private int id;
        private Thread thread;
        private String name;
        private String source;
        private String sink;
        private String channel;
        private String pattern;
        private String op;
        private String deliveryTime;
        private String addictiveString;
    }

    @Data
    public static class Manager {
        private String port;
        private String host;
    }
    
    @Data
    public static class Proxy {
        private String bid;
        private String tid;
        private Manager manager;
    }
}