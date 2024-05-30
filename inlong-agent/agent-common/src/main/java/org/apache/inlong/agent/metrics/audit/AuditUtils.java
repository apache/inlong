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

package org.apache.inlong.agent.metrics.audit;

import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.audit.AuditOperator;
import org.apache.inlong.audit.util.AuditConfig;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashSet;

import static org.apache.inlong.agent.constant.AgentConstants.AUDIT_ENABLE;
import static org.apache.inlong.agent.constant.AgentConstants.AUDIT_KEY_PROXYS;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_AUDIT_ENABLE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_AUDIT_PROXYS;
import static org.apache.inlong.audit.consts.ConfigConstants.DEFAULT_AUDIT_TAG;
import static org.apache.inlong.common.constant.Constants.DEFAULT_AUDIT_VERSION;

/**
 * AuditUtils
 */
public class AuditUtils {

    public static final String AUDIT_KEY_FILE_PATH = "audit.filePath";
    public static final String AUDIT_DEFAULT_FILE_PATH = "/data/inlong/audit/";
    public static final String AUDIT_KEY_MAX_CACHE_ROWS = "audit.maxCacheRows";
    public static final int AUDIT_DEFAULT_MAX_CACHE_ROWS = 2000000;
    public static int AUDIT_ID_AGENT_READ_SUCCESS = 3;
    public static int AUDIT_ID_AGENT_SEND_SUCCESS = 4;
    public static int AUDIT_ID_AGENT_READ_FAILED = 524291;
    public static int AUDIT_ID_AGENT_SEND_FAILED = 524292;
    public static int AUDIT_ID_AGENT_RESEND = 65540;
    public static int AUDIT_ID_AGENT_READ_SUCCESS_REAL_TIME = 1073741825;
    public static int AUDIT_ID_AGENT_SEND_SUCCESS_REAL_TIME = 1073741826;
    public static int AUDIT_ID_AGENT_READ_FAILED_REAL_TIME = 1073741827;
    public static int AUDIT_ID_AGENT_SEND_FAILED_REAL_TIME = 1073741828;
    public static int AUDIT_ID_AGENT_TRY_SEND_REAL_TIME = 1073741829;
    public static int AUDIT_ID_AGENT_SEND_EXCEPTION_REAL_TIME = 1073741830;
    public static int AUDIT_ID_AGENT_RESEND_REAL_TIME = 1073741831;
    public static int AUDIT_ID_AGENT_SEND_EXCEPTION = 1073741832;
    public static int AUDIT_ID_AGENT_TRY_SEND = 1073741833;
    public static int AUDIT_ID_AGENT_ADD_INSTANCE_DB = 1073741834;
    public static int AUDIT_ID_AGENT_DEL_INSTANCE_DB = 1073741835;
    public static int AUDIT_ID_AGENT_ADD_INSTANCE_MEM = 1073741836;
    public static int AUDIT_ID_AGENT_DEL_INSTANCE_MEM = 1073741837;
    public static int AUDIT_ID_AGENT_TASK_MGR_HEARTBEAT = 1073741838;
    public static int AUDIT_ID_AGENT_TASK_HEARTBEAT = 1073741839;
    public static int AUDIT_ID_AGENT_INSTANCE_MGR_HEARTBEAT = 1073741840;
    public static int AUDIT_ID_AGENT_INSTANCE_HEARTBEAT = 1073741841;
    public static int AUDIT_ID_AGENT_ADD_INSTANCE_MEM_FAILED = 1073741842;
    public static int AUDIT_ID_AGENT_DEL_INSTANCE_MEM_UNUSUAL = 1073741843;
    private static boolean IS_AUDIT = true;

    /**
     * Init audit config
     */
    public static void initAudit() {
        AgentConfiguration conf = AgentConfiguration.getAgentConf();
        IS_AUDIT = conf.getBoolean(AUDIT_ENABLE, DEFAULT_AUDIT_ENABLE);
        if (IS_AUDIT) {
            // AuditProxy
            String strIpPorts = conf.get(AUDIT_KEY_PROXYS, DEFAULT_AUDIT_PROXYS);
            HashSet<String> proxySet = new HashSet<>();
            if (!StringUtils.isBlank(strIpPorts)) {
                String[] ipPorts = strIpPorts.split("\\s+");
                Collections.addAll(proxySet, ipPorts);
            }
            AuditOperator.getInstance().setAuditProxy(proxySet);

            // AuditConfig
            String filePath = conf.get(AUDIT_KEY_FILE_PATH, AUDIT_DEFAULT_FILE_PATH);
            int maxCacheRow = conf.getInt(AUDIT_KEY_MAX_CACHE_ROWS, AUDIT_DEFAULT_MAX_CACHE_ROWS);
            AuditConfig auditConfig = new AuditConfig(filePath, maxCacheRow);
            AuditOperator.getInstance().setAuditConfig(auditConfig);
        }
    }

    /**
     * Add audit metric
     */
    public static void add(int auditID, String inlongGroupId, String inlongStreamId,
            long logTime, int count, long size, long version) {
        if (!IS_AUDIT) {
            return;
        }
        AuditOperator.getInstance()
                .add(auditID, DEFAULT_AUDIT_TAG, inlongGroupId, inlongStreamId, logTime, count, size, version);
    }

    public static void add(int auditID, String inlongGroupId, String inlongStreamId,
            long logTime, int count, long size) {
        add(auditID, inlongGroupId, inlongStreamId, logTime, count, size, DEFAULT_AUDIT_VERSION);
    }

    /**
     * Send audit data
     */
    public static void send() {
        if (!IS_AUDIT) {
            return;
        }
        AuditOperator.getInstance().flush();
    }
}
