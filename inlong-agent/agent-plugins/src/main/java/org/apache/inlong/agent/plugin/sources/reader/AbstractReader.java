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

package org.apache.inlong.agent.plugin.sources.reader;

import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_INLONG_STREAM_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_STREAM_ID;

import com.google.common.base.Joiner;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.plugin.Reader;
import org.apache.inlong.agent.plugin.metrics.PluginJmxMetric;
import org.apache.inlong.agent.plugin.metrics.PluginMetric;
import org.apache.inlong.agent.plugin.metrics.PluginPrometheusMetric;
import org.apache.inlong.agent.utils.ConfigUtil;

public abstract class AbstractReader implements Reader {

    private static AtomicLong metricsIndex = new AtomicLong(0);

    protected static PluginMetric readerMetric;

    protected static PluginMetric streamMetric;

    protected String inlongGroupId;

    protected String inlongStreamId;

    @Override
    public void init(JobProfile jobConf) {
        inlongGroupId = jobConf.get(PROXY_INLONG_GROUP_ID, DEFAULT_PROXY_INLONG_GROUP_ID);
        inlongStreamId = jobConf.get(PROXY_INLONG_STREAM_ID, DEFAULT_PROXY_INLONG_STREAM_ID);
    }

    protected void intMetric(String tagName) {
        String label = Joiner.on(",").join(tagName, String.valueOf(metricsIndex.getAndIncrement()));
        if (ConfigUtil.isPrometheusEnabled()) {
            readerMetric = new PluginPrometheusMetric(label);
        } else {
            readerMetric = new PluginJmxMetric(label);
        }
        label = Joiner.on(",").join(tagName, inlongGroupId, inlongStreamId);
        if (ConfigUtil.isPrometheusEnabled()) {
            streamMetric = new PluginPrometheusMetric(label);
        } else {
            streamMetric = new PluginJmxMetric(label);
        }
    }

}
