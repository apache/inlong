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

package org.apache.inlong.agent.metrics;

import org.apache.inlong.common.metric.CountMetric;
import org.apache.inlong.common.metric.Dimension;
import org.apache.inlong.common.metric.MetricDomain;
import org.apache.inlong.common.metric.MetricItem;

import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicLong;

@MetricDomain(name = "AgentEvent")
public class AgentEventMetricItem extends MetricItem {

    public final static SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final String KEY_INLONG_EVENT_TIME = "eventTime";
    public static final String KEY_INLONG_GROUP_ID = "groupId";
    public static final String KEY_INLONG_STREAM_ID = "streamId";
    public static final String KEY_INLONG_COMPONENT_TYPE = "componentType";
    public static final String KEY_INLONG_COMPONENT_NAME = "componentName";
    public static final String KEY_INLONG_AGENT_IP = "agentIp";
    public static final String KEY_INLONG_COMPONENT_VERSION = "componentVersion";
    public static final String KEY_INLONG_EVENT_TYPE = "eventType";
    public static final String KEY_INLONG_EVENT_LEVEL = "eventLevel";
    public static final String KEY_INLONG_EVENT_CODE = "eventCode";
    public static final String KEY_INLONG_EXT = "ext";
    public static final String KEY_INLONG_EVENT_DESC = "eventDesc";

    @Dimension
    public String eventTime;
    @Dimension
    public String groupId;
    @Dimension
    public String streamId;
    @Dimension
    public String componentType;
    @Dimension
    public String componentName;
    @Dimension
    public String agentIp;
    @Dimension
    public String componentVersion;
    @Dimension
    public String eventType;
    @Dimension
    public String eventLevel;
    @Dimension
    public String eventCode;
    @Dimension
    public String ext;
    @Dimension
    public String eventDesc;

    @CountMetric
    public AtomicLong count = new AtomicLong(0);
}
