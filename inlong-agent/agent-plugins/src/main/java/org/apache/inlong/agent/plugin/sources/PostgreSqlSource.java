/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.inlong.agent.plugin.sources;

import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.plugin.Reader;
import org.apache.inlong.agent.plugin.Source;
import org.apache.inlong.agent.plugin.metrics.GlobalMetrics;
import org.apache.inlong.agent.plugin.sources.reader.PostgreSqlReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_STREAM_ID;
import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_INLONG_STREAM_ID;

/**
 * PostgreSql source, split PostgreSql source job into multi readers
 */
public class PostgreSqlSource implements Source {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSqlSource.class);

    private static final String POSTGRESQL_SOURCE_TAG_NAME = "PostgreSqlSourceMetric";

    public PostgreSqlSource() {

    }

    @Override
    public List<Reader> split(JobProfile conf) {
        String inlongGroupId = conf.get(PROXY_INLONG_GROUP_ID, DEFAULT_PROXY_INLONG_GROUP_ID);
        String inlongStreamId = conf.get(PROXY_INLONG_STREAM_ID, DEFAULT_PROXY_INLONG_STREAM_ID);
        String metricTagName = POSTGRESQL_SOURCE_TAG_NAME + "_" + inlongGroupId + "_" + inlongStreamId;
        Reader postgreReader = new PostgreSqlReader();
        List<Reader> readerList = new ArrayList<>();
        readerList.add(postgreReader);
        GlobalMetrics.incSourceSuccessCount(metricTagName);
        return readerList;
    }
}
