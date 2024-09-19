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

import java.util.List;

@Data
public class FileTask {

    private Dir dir;
    private Thread thread;
    private Integer id;
    private String cycleUnit;
    private Boolean retry;
    private Long startTime;
    private Long endTime;
    private String timeOffset;
    private String timeZone;
    private String addictiveString;
    private String collectType;
    private Line line;
    private Integer maxFileCount;

    // INCREMENT
    // FULL
    private String contentCollectType;

    private String dataContentStyle;

    private String dataSeparator;

    // The streamIds to be filtered out
    private String filterStreams;

    // Monitor interval for file
    private Long monitorInterval;

    // Monitor switch, 1 true and 0 false
    private Integer monitorStatus;

    // Monitor expire time and the time in milliseconds
    private Long monitorExpire;

    @Data
    public static class Dir {

        private String patterns;

        private String blackList;
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
    public static class Line {

        private String endPattern;
    }

    @Data
    public static class FileTaskConfig {

        private String cycleUnit;

        private Boolean retry;

        private Long startTime;

        private Long endTime;

        private String pattern;

        private String blackList;
        // '1m' means one minute after, '-1m' means one minute before
        // '1h' means one hour after, '-1h' means one hour before
        // '1d' means one day after, '-1d' means one day before
        // Null means from current timestamp
        private String timeOffset;
        // For example: a=b&c=b&e=f
        private String additionalAttr;

        private String collectType;

        private String lineEndPattern;

        // Type of file content, for example: FULL, INCREMENT
        private String contentCollectType;

        // Type of data result for column separator
        // CSV format, set this parameter to a custom separator: , | :
        // Json format, set this parameter to json
        private String dataContentStyle;

        // Column separator of data source
        private String dataSeparator;

        // The streamIds to be filtered out
        private List<String> filterStreams;

        // Monitor interval for file
        private Long monitorInterval;

        // Monitor switch, 1 true and 0 false
        private Integer monitorStatus;

        // Monitor expire time and the time in milliseconds
        private Long monitorExpire;

        private Integer maxFileCount;
    }

}
