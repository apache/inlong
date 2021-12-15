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

package org.apache.inlong.agent.plugin.sinks;

import java.nio.charset.StandardCharsets;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.MessageFilter;
import org.apache.inlong.agent.plugin.Sink;
import org.apache.inlong.agent.plugin.metrics.SinkJmxMetric;
import org.apache.inlong.agent.plugin.metrics.SinkMetrics;
import org.apache.inlong.agent.plugin.metrics.SinkPrometheusMetrics;
import org.apache.inlong.agent.utils.ConfigUtil;

/**
 * message write to console
 */
public class ConsoleSink implements Sink {

    private static final String CONSOLE_SINK_TAG_NAME = "AgentConsoleSinkMetric";

    private final SinkMetrics sinkMetrics;

    public ConsoleSink() {
        if (ConfigUtil.isPrometheusEnabled()) {
            this.sinkMetrics = new SinkPrometheusMetrics(CONSOLE_SINK_TAG_NAME);
        } else {
            this.sinkMetrics = new SinkJmxMetric(CONSOLE_SINK_TAG_NAME);
        }
    }

    @Override
    public void write(Message message) {
        if (message != null) {
            System.out.println(new String(message.getBody(), StandardCharsets.UTF_8));
            // increment the count of successful sinks
            sinkMetrics.incSinkSuccessCount();
        } else {
            // increment the count of failed sinks
            sinkMetrics.incSinkFailCount();
        }
    }

    @Override
    public void setSourceFile(String sourceFileName) {

    }

    @Override
    public MessageFilter initMessageFilter(JobProfile jobConf) {
        return null;
    }

    @Override
    public void init(JobProfile jobConf) {

    }

    @Override
    public void destroy() {

    }
}
