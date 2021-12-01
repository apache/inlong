/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.inlong.agent.stats;

import io.prometheus.client.Counter;

public class SinkStatsManager {

    public static final String AGENT_SINK_METRICS_PREFIX = "inlong_agent_sink_";

    public static final String SINK_SUCCESS_COUNT_NAME = "success_count";
    public static final String SINK_FAIL_COUNT_NAME = "fail_count";

    private static final Counter SINK_SUCCESS_COUNT = Counter.build()
            .name(AGENT_SINK_METRICS_PREFIX + SINK_SUCCESS_COUNT_NAME)
            .help("The sink success message count in agent source since agent started.")
            .register();

    private static final Counter SINK_FAIL_COUNT = Counter.build()
            .name(AGENT_SINK_METRICS_PREFIX + SINK_FAIL_COUNT_NAME)
            .help("The sink failed message count in agent source since agent started.")
            .register();

    /**
     * Count the sink success message count in agent source since agent started.
     */
    public static void incrSinkSuccessCount() {
        SINK_SUCCESS_COUNT.inc();
    }

    /**
     * Get the count of successful sinks.
     */
    public static double getSinkSuccessCount() {
        return SINK_SUCCESS_COUNT.get();
    }

    /**
     * Count the sink failed message count in agent source since agent started.
     */
    public static void incrSinkFailCount() {
        SINK_FAIL_COUNT.inc();
    }

    /**
     * Get the count of failed sinks.
     */
    public static double getSinkFailCount() {
        return SINK_FAIL_COUNT.get();
    }

}
