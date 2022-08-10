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

package org.apache.inlong.agent.plugin.sources;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.plugin.Reader;
import org.apache.inlong.agent.plugin.Source;
import org.apache.inlong.agent.plugin.sources.reader.SQLServerReader;
import org.apache.inlong.agent.utils.AgentDbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.inlong.agent.constant.AgentConstants.GLOBAL_METRICS;
import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_INLONG_STREAM_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_STREAM_ID;

public class SQLServerSource implements Source {

    private static final Logger logger = LoggerFactory.getLogger(SQLServerSource.class);

    private static final String SQLSERVER_SOURCE_TAG_NAME = "SQLServerSourceMetric";

    public static final String JOB_DATABASE_SQL = "job.sql.command";

    public SQLServerSource() {
    }

    private List<Reader> splitSqlJob(String sqlPattern) {
        final List<Reader> result = new ArrayList<>();
        String[] sqlList = AgentDbUtils.replaceDynamicSeq(sqlPattern);
        if (Objects.nonNull(sqlList)) {
            Arrays.stream(sqlList).forEach(sql -> {
                result.add(new SQLServerReader(sql));
            });
        }
        return result;
    }

    @Override
    public List<Reader> split(JobProfile conf) {
        String inlongGroupId = conf.get(PROXY_INLONG_GROUP_ID, DEFAULT_PROXY_INLONG_GROUP_ID);
        String inlongStreamId = conf.get(PROXY_INLONG_STREAM_ID, DEFAULT_PROXY_INLONG_STREAM_ID);
        String metricTagName = String.join("_", SQLSERVER_SOURCE_TAG_NAME, inlongGroupId, inlongStreamId);
        String sqlPattern = conf.get(JOB_DATABASE_SQL, StringUtils.EMPTY).toLowerCase();
        List<Reader> readerList = null;
        if (StringUtils.isNotEmpty(sqlPattern)) {
            readerList = splitSqlJob(sqlPattern);
        }
        if (CollectionUtils.isNotEmpty(readerList)) {
            GLOBAL_METRICS.incSourceSuccessCount(metricTagName);
        } else {
            GLOBAL_METRICS.incSourceFailCount(metricTagName);
        }
        return readerList;
    }
}
