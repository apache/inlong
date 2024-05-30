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

package org.apache.inlong.audit;

import org.apache.inlong.audit.util.AuditManagerUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metric item management, each module is assigned two baseline audit item IDs, namely receiving and sending.
 */
public enum MetricIdEnum {

    AGENT_READ_SUCCESS_REAL_TIME(1, "Agent 读取成功-监控", "Agent read success-metric"),
    AGENT_SEND_SUCCESS_REAL_TIME(2, "Agent 发送成功-监控", "Agent send success-metric"),
    AGENT_READ_FAILED_REAL_TIME(3, "Agent 读取失败-监控", "Agent read failed-metric"),
    AGENT_SEND_FAILED_REAL_TIME(4, "Agent 发送失败-监控", "Agent send failed-metric"),
    AGENT_TRY_SEND_REAL_TIME(5, "Agent 尝试发送-监控", "Agent try send-metric"),
    AGENT_SEND_EXCEPTION_REAL_TIME(6, "Agent 发送异常-监控", "Agent send exception-metric"),
    AGENT_RESEND_REAL_TIME(7, "Agent 重发-监控", "Agent resend-metric"),
    AGENT_SEND_EXCEPTION(8, "Agent 发送异常", "Agent send exception"),
    AGENT_TRY_SEND(9, "Agent 尝试发送", "Agent try send"),
    AGENT_ADD_INSTANCE_DB(10, "Agent 增加实例-DB", "Agent add instance-DB"),
    AGENT_DEL_INSTANCE_DB(11, "Agent 删除实例-DB", "Agent delete instance-DB"),
    AGENT_ADD_INSTANCE_MEM(12, "Agent 增加实例-内存", "Agent add instance-mem"),
    AGENT_DEL_INSTANCE_MEM(13, "Agent 删除实例-内存", "Agent delete instance-mem"),
    AGENT_TASK_MGR_HEARTBEAT(14, "Agent 任务管理器心跳", "Agent task manager heartbeat"),
    AGENT_TASK_HEARTBEAT(15, "Agent 任务心跳", "Agent task heartbeat"),
    AGENT_INSTANCE_MGR_HEARTBEAT(16, "Agent 实例管理器心跳", "Agent instance manager heartbeat"),
    AGENT_INSTANCE_HEARTBEAT(17, "Agent 实例心跳", "Agent instance heartbeat"),
    AGENT_ADD_INSTANCE_MEM_FAILED(18, "Agent 增加实例失败-内存", "Agent add instance failed-mem"),
    AGENT_DEL_INSTANCE_MEM_UNUSUAL(19, "Agent 删除实例异常-内存", "Agent delete instance unusual-mem");

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricIdEnum.class);
    private final int metricId;
    private final String chineseDescription;
    private final String englishDescription;

    MetricIdEnum(int metricId, String chineseDescription, String englishDescription) {
        this.metricId = metricId;
        this.chineseDescription = chineseDescription;
        this.englishDescription = englishDescription;
    }

    public int getValue() {
        return metricId + AuditManagerUtils.getStartAuditIdForMetric();
    }

    public String getChineseDescription() {
        return chineseDescription;
    }

    public String getEnglishDescription() {
        return englishDescription;
    }
}
