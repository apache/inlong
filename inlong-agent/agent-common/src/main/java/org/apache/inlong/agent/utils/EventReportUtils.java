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

package org.apache.inlong.agent.utils;

import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.metrics.AgentEventMetricItem;
import org.apache.inlong.agent.metrics.AgentEventMetricItemSet;
import org.apache.inlong.common.metric.MetricRegister;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.inlong.agent.constant.AgentConstants.AGENT_LOCAL_IP;
import static org.apache.inlong.agent.metrics.AgentEventMetricItem.KEY_INLONG_AGENT_IP;
import static org.apache.inlong.agent.metrics.AgentEventMetricItem.KEY_INLONG_COMPONENT_NAME;
import static org.apache.inlong.agent.metrics.AgentEventMetricItem.KEY_INLONG_COMPONENT_TYPE;
import static org.apache.inlong.agent.metrics.AgentEventMetricItem.KEY_INLONG_COMPONENT_VERSION;
import static org.apache.inlong.agent.metrics.AgentEventMetricItem.KEY_INLONG_EVENT_CODE;
import static org.apache.inlong.agent.metrics.AgentEventMetricItem.KEY_INLONG_EVENT_DESC;
import static org.apache.inlong.agent.metrics.AgentEventMetricItem.KEY_INLONG_EVENT_LEVEL;
import static org.apache.inlong.agent.metrics.AgentEventMetricItem.KEY_INLONG_EVENT_TIME;
import static org.apache.inlong.agent.metrics.AgentEventMetricItem.KEY_INLONG_EVENT_TYPE;
import static org.apache.inlong.agent.metrics.AgentEventMetricItem.KEY_INLONG_EXT;
import static org.apache.inlong.agent.metrics.AgentEventMetricItem.KEY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.metrics.AgentEventMetricItem.KEY_INLONG_STREAM_ID;

/**
 * DiagUtils
 */
public class EventReportUtils {

    public enum EvenCodeEnum {

        CONFIG_UPDATE_SUC(0, "config update suc"),
        CONFIG_NO_UPDATE(1, "config no update"),
        CONFIG_UPDATE_VERSION_NO_CHANGE(2, "config update version no change"),
        CONFIG_INVALID_RET_CODE(3, "config invalid ret code"),
        CONFIG_INVALID_RESULT(4, "config invalid result maybe visit manager failed"),
        TASK_ADD(5, "task add"),
        TASK_DELETE(6, "task delete");

        private final int code;
        private final String message;

        EvenCodeEnum(int code, String message) {
            this.code = code;
            this.message = message;
        }

        public int getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }
    }

    private final static String COMPONENT_TYPE_AGENT = "AGENT";
    private final static String COMPONENT_NAME_AGENT = "AGENT";
    public static final String EVENT_TYPE_CONFIG_UPDATE = "CONFIG_UPDATE";
    public static final String EVENT_LEVEL_INFO = "INFO";
    public static final String EVENT_LEVEL_WARN = "WARN";
    public static final String EVENT_LEVEL_ERROR = "ERROR";
    private static AgentEventMetricItemSet metricItemSet;

    private EventReportUtils() {
    }

    public static void init() {
        metricItemSet = new AgentEventMetricItemSet(COMPONENT_NAME_AGENT);
        MetricRegister.register(metricItemSet);
    }

    public static void report(String groupId, String streamId, long eventTime, String eventType,
            String eventLevel, EvenCodeEnum evenCode, String ext, String desc) {
        Map<String, String> dims = new HashMap<>();
        dims.put(KEY_INLONG_GROUP_ID, groupId);
        dims.put(KEY_INLONG_STREAM_ID, streamId);
        dims.put(KEY_INLONG_COMPONENT_TYPE, COMPONENT_TYPE_AGENT);
        dims.put(KEY_INLONG_COMPONENT_NAME, COMPONENT_NAME_AGENT);
        dims.put(KEY_INLONG_AGENT_IP, AgentConfiguration.getAgentConf().get(AGENT_LOCAL_IP));
        dims.put(KEY_INLONG_COMPONENT_VERSION, EventReportUtils.class.getPackage().getImplementationVersion());
        dims.put(KEY_INLONG_EVENT_TIME, AgentEventMetricItem.FORMAT.format(new Date(eventTime)));
        dims.put(KEY_INLONG_EVENT_TYPE, eventType);
        dims.put(KEY_INLONG_EVENT_LEVEL, eventLevel);
        dims.put(KEY_INLONG_EVENT_CODE, String.valueOf(evenCode.getCode()));
        dims.put(KEY_INLONG_EXT, ext.replaceAll("\\|", "-"));
        dims.put(KEY_INLONG_EVENT_DESC, desc);
        metricItemSet.findMetricItem(dims).count.addAndGet(1);
    }
}
