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
public class SQLTask {

    private Integer id;
    private String sql;
    private String cycleUnit;
    private Boolean retry;
    private String dataTimeFrom;
    private String dataTimeTo;
    private String timeOffset;
    private Integer maxInstanceCount;
    private String jdbcUrl;
    private String username;
    private String jdbcPassword;
    private String dataSeparator;
    private Integer fetchSize;

    @Data
    public static class SQLTaskConfig {

        private String sql;
        private String cycleUnit;
        private Boolean retry;
        private String dataTimeFrom;
        private String dataTimeTo;
        // '1m' means one minute after, '-1m' means one minute before
        // '1h' means one hour after, '-1h' means one hour before
        // '1d' means one day after, '-1d' means one day before
        // Null means from current timestamp
        private String timeOffset;
        private Integer maxInstanceCount;
        private String jdbcUrl;
        private String username;
        private String jdbcPassword;
        private String dataSeparator;
        private Integer fetchSize;
    }
}
