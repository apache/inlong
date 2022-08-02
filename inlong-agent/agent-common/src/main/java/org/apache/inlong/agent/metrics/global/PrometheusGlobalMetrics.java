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

package org.apache.inlong.agent.metrics.global;

import org.apache.inlong.agent.metrics.plugin.PluginMetric;
import org.apache.inlong.agent.metrics.plugin.PluginPrometheusMetric;
import org.apache.inlong.agent.metrics.sink.SinkMetric;
import org.apache.inlong.agent.metrics.sink.SinkPrometheusMetric;
import org.apache.inlong.agent.metrics.source.SourceMetric;
import org.apache.inlong.agent.metrics.source.SourcePrometheusMetric;

public class PrometheusGlobalMetrics extends GlobalMetrics {

    @Override
    protected PluginMetric addPluginMetric(String tagName) {
        return new PluginPrometheusMetric(tagName);
    }

    @Override
    protected SourceMetric addSourceMetric(String tagName) {
        return new SourcePrometheusMetric(tagName);
    }

    @Override
    protected SinkMetric addSinkMetric(String tagName) {
        return new SinkPrometheusMetric(tagName);
    }
}
