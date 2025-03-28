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

package org.apache.inlong.agent.core;

import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpEventInfo;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSender;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_NAME;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_TAG;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_LOCAL_IP;

/**
 * Collect various indicators of agent processes for backend problem analysis
 */
public class FileStaticManager {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class FileStatic {

        private String agentIp;
        private String tag;
        private String cluster;
        private String taskId;
        private String retry;
        private String contentType;
        private String groupId;
        private String streamId;
        private String dataTime;
        private String fileName;
        private String fileLen;
        private String readBytes;
        private String readLines;
        private String sendLines;

        public String getFieldsString() {
            List<String> fields = Lists.newArrayList();
            fields.add(agentIp);
            fields.add(tag);
            fields.add(cluster);
            fields.add(taskId);
            fields.add(retry);
            fields.add(contentType);
            fields.add(groupId);
            fields.add(streamId);
            fields.add(dataTime);
            fields.add(fileName);
            fields.add(fileLen);
            fields.add(readBytes);
            fields.add(readLines);
            fields.add(sendLines);
            return Strings.join(fields, ',');
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(FileStaticManager.class);
    public static final String INLONG_AGENT_SYSTEM = "inlong_agent_system";
    public static final String INLONG_FILE_STATIC = "inlong_agent_file_static";
    protected final Integer CACHE_QUEUE_SIZE = 10000;
    private static FileStaticManager manager = null;
    private final AgentConfiguration conf;
    protected BlockingQueue<FileStatic> queue;

    private FileStaticManager() {
        this.conf = AgentConfiguration.getAgentConf();
        queue = new LinkedBlockingQueue<>(CACHE_QUEUE_SIZE);
    }

    public static void init() {
        synchronized (FileStaticManager.class) {
            if (manager == null) {
                manager = new FileStaticManager();
            }
        }
    }

    private static FileStaticManager getInstance() {
        return manager;
    }

    private void doPutStaticMsg(FileStatic data) {
        data.setAgentIp(conf.get(AGENT_LOCAL_IP));
        data.setTag(conf.get(AGENT_CLUSTER_TAG));
        data.setCluster(conf.get(AGENT_CLUSTER_NAME));
        while (!queue.offer(data)) {
            LOGGER.error("file static queue is full remove {}", queue.poll());
        }
    }

    public static void putStaticMsg(FileStatic data) {
        if (FileStaticManager.getInstance() != null) {
            FileStaticManager.getInstance().doPutStaticMsg(data);
        }
    }

    private void doSendStaticMsg(TcpMsgSender sender) {
        while (!queue.isEmpty()) {
            FileStatic data = queue.poll();
            LOGGER.info("file static detail: {}", data);
            if (sender == null) {
                continue;
            }
            try {
                ProcessResult procResult = new ProcessResult();
                long dataTime = AgentUtils.getCurrentTime();
                byte[] body = data.getFieldsString().getBytes(StandardCharsets.UTF_8);
                if (!sender.sendMessage(new TcpEventInfo(INLONG_AGENT_SYSTEM,
                        INLONG_FILE_STATIC, dataTime, null, body), procResult)) {
                    LOGGER.error("send static failed: ret = {}", procResult);
                    AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_SEND_FAILED, INLONG_AGENT_SYSTEM, INLONG_FILE_STATIC,
                            dataTime, 1, body.length);
                    AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_SEND_FAILED_REAL_TIME, INLONG_AGENT_SYSTEM,
                            INLONG_FILE_STATIC, dataTime, 1, body.length);
                } else {
                    AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_SEND_SUCCESS, INLONG_AGENT_SYSTEM, INLONG_FILE_STATIC,
                            dataTime, 1, body.length);
                    AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_SEND_SUCCESS_REAL_TIME, INLONG_AGENT_SYSTEM,
                            INLONG_FILE_STATIC, dataTime, 1, body.length);
                }
            } catch (Throwable ex) {
                LOGGER.error("send static throw exception", ex);
            }
        }
    }

    public static void sendStaticMsg(TcpMsgSender sender) {
        if (FileStaticManager.getInstance() != null) {
            FileStaticManager.getInstance().doSendStaticMsg(sender);
        }
    }
}