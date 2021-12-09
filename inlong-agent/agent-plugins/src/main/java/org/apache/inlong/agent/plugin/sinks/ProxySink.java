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

import static org.apache.inlong.agent.constants.CommonConstants.DEFAULT_FIELD_SPLITTER;
import static org.apache.inlong.agent.constants.CommonConstants.PROXY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.constants.CommonConstants.PROXY_KEY_AGENT_IP;
import static org.apache.inlong.agent.constants.CommonConstants.PROXY_KEY_ID;
import static org.apache.inlong.agent.constants.CommonConstants.PROXY_OCEANUS_BL;
import static org.apache.inlong.agent.constants.CommonConstants.PROXY_OCEANUS_F;
import static org.apache.inlong.agent.constants.CommonConstants.PROXY_INLONG_STREAM_ID;
import static org.apache.inlong.agent.constants.JobConstants.PROXY_BATCH_FLUSH_INTERVAL;
import static org.apache.inlong.agent.constants.JobConstants.PROXY_PACKAGE_MAX_SIZE;
import static org.apache.inlong.agent.constants.JobConstants.PROXY_PACKAGE_MAX_TIMEOUT_MS;
import static org.apache.inlong.agent.constants.JobConstants.PROXY_INLONG_STREAM_ID_QUEUE_MAX_NUMBER;
import static org.apache.inlong.agent.constants.JobConstants.DEFAULT_PROXY_BATCH_FLUSH_INTERVAL;
import static org.apache.inlong.agent.constants.JobConstants.DEFAULT_PROXY_PACKAGE_MAX_SIZE;
import static org.apache.inlong.agent.constants.JobConstants.DEFAULT_PROXY_PACKAGE_MAX_TIMEOUT_MS;
import static org.apache.inlong.agent.constants.JobConstants.DEFAULT_PROXY_INLONG_STREAM_ID_QUEUE_MAX_NUMBER;
import static org.apache.inlong.agent.constants.JobConstants.JOB_ADDITION_STR;
import static org.apache.inlong.agent.constants.JobConstants.JOB_CYCLE_UNIT;
import static org.apache.inlong.agent.constants.JobConstants.JOB_DATA_TIME;
import static org.apache.inlong.agent.constants.JobConstants.JOB_ID;
import static org.apache.inlong.agent.constants.JobConstants.JOB_INSTANCE_ID;
import static org.apache.inlong.agent.constants.JobConstants.JOB_IP;
import static org.apache.inlong.agent.constants.JobConstants.JOB_RETRY;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.constants.CommonConstants;
import org.apache.inlong.agent.message.ProxyMessage;
import org.apache.inlong.agent.message.EndMessage;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.MessageFilter;
import org.apache.inlong.agent.plugin.message.PackProxyMessage;
import org.apache.inlong.agent.plugin.metrics.PluginJmxMetric;
import org.apache.inlong.agent.plugin.metrics.PluginPrometheusMetric;
import org.apache.inlong.agent.plugin.metrics.SinkJmxMetric;
import org.apache.inlong.agent.plugin.metrics.SinkMetrics;
import org.apache.inlong.agent.plugin.metrics.SinkPrometheusMetrics;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxySink extends AbstractSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxySink.class);

    private static final String PROXY_SINK_TAG_NAME = "AgentProxySinkMetric";

    private MessageFilter messageFilter;
    private SenderManager senderManager;
    private byte[] fieldSplitter;
    private String inlongGroupId;
    private String inlongStreamId;
    private String sourceFile;
    private String jobInstanceId;
    private int maxBatchSize;
    private int maxBatchTimeoutMs;
    private int batchFlushInterval;
    private int maxQueueNumber;
    private final ExecutorService executorService = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), new AgentThreadFactory("ProxySink"));
    private volatile boolean shutdown = false;

    // key is stream id, value is a batch of messages belong to the same stream id
    private ConcurrentHashMap<String, PackProxyMessage> cache;
    private long dataTime;

    private final SinkMetrics sinkMetrics;

    public ProxySink() {
        if (ConfigUtil.isPrometheusEnabled()) {
            this.sinkMetrics = new SinkPrometheusMetrics(PROXY_SINK_TAG_NAME);
        } else {
            this.sinkMetrics = new SinkJmxMetric(PROXY_SINK_TAG_NAME);
        }
    }

    @Override
    public void write(Message message) {
        if (message != null) {
            message.getHeader().put(CommonConstants.PROXY_KEY_GROUP_ID, inlongStreamId);
            extractStreamFromMessage(message, fieldSplitter);
            if (!(message instanceof EndMessage)) {
                ProxyMessage proxyMessage = ProxyMessage.parse(message);
                    // add proxy message to cache.
                cache.compute(proxyMessage.getInlongStreamId(),
                    (s, packProxyMessage) -> {
                        if (packProxyMessage == null) {
                            packProxyMessage = new PackProxyMessage(
                            maxBatchSize, maxQueueNumber,
                            maxBatchTimeoutMs, proxyMessage.getInlongStreamId());
                        }
                        // add message to package proxy
                        packProxyMessage.addProxyMessage(proxyMessage);
                        //
                        return packProxyMessage;
                    });
                // increment the count of successful sinks
                sinkMetrics.incSinkSuccessCount();
            } else {
                // increment the count of failed sinks
                sinkMetrics.incSinkFailCount();
            }
        }
    }

    /**
     * extract stream id from message if message filter is presented
     * or use the default stream id
     * @param message
     */
    private void extractStreamFromMessage(Message message, byte[] fieldSplitter) {
        if (messageFilter != null) {
            message.getHeader().put(CommonConstants.PROXY_KEY_STREAM_ID,
                messageFilter.filterStreamId(message, fieldSplitter));
        } else {
            message.getHeader().put(CommonConstants.PROXY_KEY_STREAM_ID, inlongStreamId);
        }
    }

    @Override
    public void setSourceFile(String sourceFileName) {
        this.sourceFile = sourceFileName;
    }

    /**
     * flush cache by batch
     *
     * @return - thread runner
     */
    private Runnable flushCache() {
        return () -> {
            LOGGER.info("start flush cache thread for {} ProxySink", inlongStreamId);
            while (!shutdown) {
                try {
                    cache.forEach((s, packProxyMessage) -> {
                        Pair<String, List<byte[]>> result = packProxyMessage.fetchBatch();
                        if (result != null) {
                            senderManager.sendBatch(jobInstanceId, inlongStreamId, result.getKey(),
                                    result.getValue(), 0, dataTime);
                            LOGGER.info("send group id {} with message size {}, the job id is {}, read file is {}"
                                    + "dataTime is {}", inlongStreamId, result.getRight().size(),
                                jobInstanceId, sourceFile, dataTime);
                        }

                    });
                    AgentUtils.silenceSleepInMs(batchFlushInterval);
                } catch (Exception ex) {
                    LOGGER.error("error caught", ex);
                }
            }
        };
    }

    @Override
    public void init(JobProfile jobConf) {
        maxBatchSize = jobConf.getInt(PROXY_PACKAGE_MAX_SIZE, DEFAULT_PROXY_PACKAGE_MAX_SIZE);
        maxQueueNumber = jobConf.getInt(PROXY_INLONG_STREAM_ID_QUEUE_MAX_NUMBER,
            DEFAULT_PROXY_INLONG_STREAM_ID_QUEUE_MAX_NUMBER);
        maxBatchTimeoutMs = jobConf.getInt(
            PROXY_PACKAGE_MAX_TIMEOUT_MS, DEFAULT_PROXY_PACKAGE_MAX_TIMEOUT_MS);
        jobInstanceId = jobConf.get(JOB_INSTANCE_ID);
        batchFlushInterval = jobConf.getInt(PROXY_BATCH_FLUSH_INTERVAL,
            DEFAULT_PROXY_BATCH_FLUSH_INTERVAL);
        cache = new ConcurrentHashMap<>(10);
        inlongStreamId = jobConf.get(PROXY_INLONG_GROUP_ID);
        dataTime = AgentUtils.timeStrConvertToMillSec(jobConf.get(JOB_DATA_TIME, ""),
            jobConf.get(JOB_CYCLE_UNIT, ""));
        inlongGroupId = jobConf.get(PROXY_INLONG_GROUP_ID);
        inlongStreamId = jobConf.get(PROXY_INLONG_STREAM_ID, "");
        messageFilter = initMessageFilter(jobConf);
        fieldSplitter = jobConf.get(CommonConstants.FIELD_SPLITTER, DEFAULT_FIELD_SPLITTER).getBytes(
            StandardCharsets.UTF_8);
        executorService.execute(flushCache());
        senderManager = new SenderManager(jobConf, inlongStreamId, sourceFile);
        try {
            senderManager.addMessageSender();
        } catch (Exception ex) {
            LOGGER.error("error while init sender for group id {}", inlongStreamId);
            throw new IllegalStateException(ex);
        }
    }

    private HashMap<String, String> parseAttrFromJobProfile(JobProfile jobProfile) {
        HashMap<String, String> attr = new HashMap<>();
        String additionStr = jobProfile.get(JOB_ADDITION_STR, "");
        if (!additionStr.isEmpty()) {
            Map<String, String> addAttr = AgentUtils.getAdditionAttr(additionStr);
            attr.putAll(addAttr);
        }
        if (jobProfile.getBoolean(JOB_RETRY, false)) {
            // used for online compute filter consume
            attr.put(PROXY_OCEANUS_F, PROXY_OCEANUS_BL);
        }
        attr.put(PROXY_KEY_ID, jobProfile.get(JOB_ID));
        attr.put(PROXY_KEY_AGENT_IP, jobProfile.get(JOB_IP));
        return attr;
    }

    @Override
    public void destroy() {
        LOGGER.info("destroy sink which sink from source file {}", sourceFile);
        while (!sinkFinish()) {
            LOGGER.info("job {} wait until cache all flushed to proxy", jobInstanceId);
            AgentUtils.silenceSleepInMs(batchFlushInterval);
        }
        shutdown = true;
        executorService.shutdown();
    }

    /**
     * check whether all stream id messages finished
     * @return
     */
    private boolean sinkFinish() {
        return cache.values().stream().allMatch(PackProxyMessage::isEmpty);
    }
}
