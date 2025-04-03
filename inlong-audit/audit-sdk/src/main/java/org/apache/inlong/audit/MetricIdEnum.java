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

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metric item management, each module is assigned two baseline audit item IDs, namely receiving and sending.
 */
public enum MetricIdEnum {

    AGENT_READ_SUCCESS_REAL_TIME(1, "Agent 读取成功-监控", "Agent Read Success-Metric"),
    AGENT_SEND_SUCCESS_REAL_TIME(2, "Agent 发送成功-监控", "Agent Send Success-Metric"),
    AGENT_READ_FAILED_REAL_TIME(3, "Agent 读取失败-监控", "Agent Read Failed-Metric"),
    AGENT_SEND_FAILED_REAL_TIME(4, "Agent 发送失败-监控", "Agent Send Failed-Metric"),
    AGENT_TRY_SEND_REAL_TIME(5, "Agent 尝试发送-监控", "Agent Try Send-Metric"),
    AGENT_SEND_EXCEPTION_REAL_TIME(6, "Agent 发送异常-监控", "Agent Send Exception-Metric"),
    AGENT_RESEND_REAL_TIME(7, "Agent 重发-监控", "Agent Resend-Metric"),
    AGENT_SEND_EXCEPTION(8, "Agent 发送异常", "Agent Send Exception"),
    AGENT_TRY_SEND(9, "Agent 尝试发送", "Agent Try Send"),
    AGENT_ADD_INSTANCE_DB(10, "Agent 增加实例-DB", "Agent Add Instance-DB"),
    AGENT_DEL_INSTANCE_DB(11, "Agent 删除实例-DB", "Agent Delete Instance-DB"),
    AGENT_ADD_INSTANCE_MEM(12, "Agent 增加实例-内存", "Agent Add Instance-Mem"),
    AGENT_DEL_INSTANCE_MEM(13, "Agent 删除实例-内存", "Agent Delete Instance-Mem"),
    AGENT_TASK_MGR_HEARTBEAT(14, "Agent 任务管理器心跳", "Agent Task Manager Heartbeat"),
    AGENT_TASK_HEARTBEAT(15, "Agent 任务心跳", "Agent Task Heartbeat"),
    AGENT_INSTANCE_MGR_HEARTBEAT(16, "Agent 实例管理器心跳", "Agent Instance Manager Heartbeat"),
    AGENT_INSTANCE_HEARTBEAT(17, "Agent 实例心跳", "Agent Instance Heartbeat"),
    AGENT_ADD_INSTANCE_MEM_FAILED(18, "Agent 增加实例失败-内存", "Agent add Instance Failed-Mem"),
    AGENT_DEL_INSTANCE_MEM_UNUSUAL(19, "Agent 删除实例异常-内存", "Agent Delete Instance Unusual-Mem");

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricIdEnum.class);
    private final int metricId;
    @Getter
    private final String chineseDescription;
    @Getter
    private final String englishDescription;

    MetricIdEnum(int metricId, String chineseDescription, String englishDescription) {
        this.metricId = metricId;
        this.chineseDescription = chineseDescription;
        this.englishDescription = englishDescription;
    }

    public int getValue() {
        return metricId + AuditManagerUtils.getStartAuditIdForMetric();
    }

}
