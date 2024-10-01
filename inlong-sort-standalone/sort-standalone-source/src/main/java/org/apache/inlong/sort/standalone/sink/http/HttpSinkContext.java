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

package org.apache.inlong.sort.standalone.sink.http;

import org.apache.inlong.common.pojo.sort.ClusterTagConfig;
import org.apache.inlong.common.pojo.sort.TaskConfig;
import org.apache.inlong.common.pojo.sort.node.HttpNodeConfig;
import org.apache.inlong.common.pojo.sortstandalone.SortTaskConfig;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.holder.SortClusterConfigHolder;
import org.apache.inlong.sort.standalone.config.holder.v2.SortConfigHolder;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;
import org.apache.inlong.sort.standalone.dispatch.DispatchProfile;
import org.apache.inlong.sort.standalone.metrics.SortConfigMetricReporter;
import org.apache.inlong.sort.standalone.metrics.SortMetricItem;
import org.apache.inlong.sort.standalone.metrics.audit.AuditUtils;
import org.apache.inlong.sort.standalone.sink.SinkContext;
import org.apache.inlong.sort.standalone.utils.BufferQueue;
import org.apache.inlong.sort.standalone.utils.Constants;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.ClassUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@SuppressWarnings("deprecation")
public class HttpSinkContext extends SinkContext {

    public static final Logger LOG = InlongLoggerFactory.getLogger(HttpSinkContext.class);
    public static final String KEY_NODE_ID = "nodeId";
    public static final String KEY_BASE_URL = "baseUrl";
    public static final String KEY_ENABLE_CREDENTIAL = "enableCredential";
    public static final String KEY_USERNAME = "username";
    public static final String KEY_PASSWORD = "password";
    public static final String KEY_MAX_CONNECT_TOTAL = "maxConnect";
    public static final String KEY_MAX_CONNECT_PER_ROUTE = "maxConnectPerRoute";
    public static final String KEY_CONNECTION_REQUEST_TIMEOUT = "connectionRequestTimeout";
    public static final String KEY_SOCKET_TIMEOUT = "socketTimeout";
    public static final String KEY_MAX_REDIRECTS = "maxRedirects";
    public static final String KEY_LOG_MAX_LENGTH = "logMaxLength";
    public static final String KEY_EVENT_HTTP_REQUEST_HANDLER = "httpRequestHandler";

    public static final boolean DEFAULT_ENABLE_CREDENTIAL = false;
    public static final int DEFAULT_MAX_CONNECT_TOTAL = 1000;
    public static final int DEFAULT_MAX_CONNECT_PER_ROUTE = 1000;
    public static final int DEFAULT_CONNECTION_REQUEST_TIMEOUT = 0;
    public static final int DEFAULT_SOCKET_TIMEOUT = 0;
    public static final int DEFAULT_MAX_REDIRECTS = 0;
    public static final int DEFAULT_LOG_MAX_LENGTH = 32 * 1024;

    private Context sinkContext;
    private HttpNodeConfig httpNodeConfig;
    private String nodeId;
    private Map<String, HttpIdConfig> idConfigMap = new ConcurrentHashMap<>();
    private ObjectMapper objectMapper = new ObjectMapper();
    private final BufferQueue<DispatchProfile> dispatchQueue;
    private AtomicLong offerCounter = new AtomicLong(0);
    private AtomicLong takeCounter = new AtomicLong(0);
    private AtomicLong backCounter = new AtomicLong(0);
    // rest client
    private String baseUrl;
    private boolean enableCredential;
    private String username;
    private String password;
    private int maxConnect = DEFAULT_MAX_CONNECT_TOTAL;
    private int maxConnectPerRoute = DEFAULT_MAX_CONNECT_PER_ROUTE;
    private int connectionRequestTimeout = DEFAULT_CONNECTION_REQUEST_TIMEOUT;
    private int socketTimeout = DEFAULT_SOCKET_TIMEOUT;
    private int maxRedirects = DEFAULT_MAX_REDIRECTS;
    private int logMaxLength = DEFAULT_LOG_MAX_LENGTH;

    public HttpSinkContext(String sinkName, Context context, Channel channel,
            BufferQueue<DispatchProfile> dispatchQueue) {
        super(sinkName, context, channel);
        this.sinkContext = context;
        this.dispatchQueue = dispatchQueue;
        this.nodeId = CommonPropertiesHolder.getString(KEY_NODE_ID);
    }

    public void reload() {
        try {
            LOG.info("SortTask:{},dispatchQueue:{},offer:{},take:{},back:{}",
                    taskName, dispatchQueue.size(), offerCounter.getAndSet(0),
                    takeCounter.getAndSet(0), backCounter.getAndSet(0));
            TaskConfig newTaskConfig = SortConfigHolder.getTaskConfig(taskName);
            SortTaskConfig newSortTaskConfig = SortClusterConfigHolder.getTaskConfig(taskName);
            if ((newTaskConfig == null || newTaskConfig.equals(taskConfig))
                    && (newSortTaskConfig == null || newSortTaskConfig.equals(sortTaskConfig))) {
                return;
            }
            LOG.info("get new SortTaskConfig:taskName:{}", taskName);

            if (newTaskConfig != null) {
                HttpNodeConfig requestNodeConfig = (HttpNodeConfig) newTaskConfig.getNodeConfig();
                if (httpNodeConfig == null || requestNodeConfig.getVersion() > httpNodeConfig.getVersion()) {
                    this.httpNodeConfig = requestNodeConfig;
                }
            }

            this.taskConfig = newTaskConfig;
            this.sortTaskConfig = newSortTaskConfig;

            // change current config
            Map<String, HttpIdConfig> fromTaskConfig = reloadIdParamsFromTaskConfig(taskConfig);
            Map<String, HttpIdConfig> fromSortTaskConfig = reloadIdParamsFromSortTaskConfig(sortTaskConfig);
            if (unifiedConfiguration) {
                idConfigMap = fromTaskConfig;
                reloadClientsFromNodeConfig(httpNodeConfig);
            } else {
                idConfigMap = fromSortTaskConfig;
                reloadClientsFromSortTaskConfig(sortTaskConfig);
            }
            SortConfigMetricReporter.reportClusterDiff(clusterId, taskName, fromTaskConfig, fromSortTaskConfig);
            // log
            LOG.info("End to get SortTaskConfig:taskName:{}:newIdConfigMap:{}", taskName,
                    objectMapper.writeValueAsString(idConfigMap));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private Map<String, HttpIdConfig> reloadIdParamsFromTaskConfig(TaskConfig taskConfig) {
        if (taskConfig == null) {
            return new HashMap<>();
        }
        return taskConfig.getClusterTagConfigs()
                .stream()
                .map(ClusterTagConfig::getDataFlowConfigs)
                .flatMap(Collection::stream)
                .map(HttpIdConfig::create)
                .collect(Collectors.toMap(
                        config -> InlongId.generateUid(config.getInlongGroupId(), config.getInlongStreamId()),
                        v -> v,
                        (flow1, flow2) -> flow1));
    }

    private Map<String, HttpIdConfig> reloadIdParamsFromSortTaskConfig(SortTaskConfig sortTaskConfig)
            throws JsonProcessingException {
        if (sortTaskConfig == null) {
            return new HashMap<>();
        }
        Map<String, HttpIdConfig> newIdConfigMap = new ConcurrentHashMap<>();
        List<Map<String, String>> idList = this.sortTaskConfig.getIdParams();
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        for (Map<String, String> idParam : idList) {
            String inlongGroupId = idParam.get(Constants.INLONG_GROUP_ID);
            String inlongStreamId = idParam.get(Constants.INLONG_STREAM_ID);
            String uid = InlongId.generateUid(inlongGroupId, inlongStreamId);
            String jsonIdConfig = objectMapper.writeValueAsString(idParam);
            HttpIdConfig idConfig = objectMapper.readValue(jsonIdConfig, HttpIdConfig.class);
            newIdConfigMap.put(uid, idConfig);
        }
        return newIdConfigMap;
    }

    private void reloadClientsFromNodeConfig(HttpNodeConfig httpNodeConfig) {
        Map<String, String> properties = httpNodeConfig.getProperties();
        this.sinkContext = new Context(properties != null ? properties : new HashMap<>());
        this.baseUrl = httpNodeConfig.getBaseUrl();
        this.enableCredential = httpNodeConfig.getEnableCredential();;
        this.username = httpNodeConfig.getUsername();
        this.password = httpNodeConfig.getPassword();
        this.maxConnect = httpNodeConfig.getMaxConnect();

        this.maxConnectPerRoute = sinkContext.getInteger(KEY_MAX_CONNECT_PER_ROUTE, DEFAULT_MAX_CONNECT_PER_ROUTE);
        this.connectionRequestTimeout = sinkContext.getInteger(KEY_CONNECTION_REQUEST_TIMEOUT,
                DEFAULT_CONNECTION_REQUEST_TIMEOUT);
        this.socketTimeout = sinkContext.getInteger(KEY_SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT);
        this.maxRedirects = sinkContext.getInteger(KEY_MAX_REDIRECTS, DEFAULT_MAX_REDIRECTS);
        this.logMaxLength = sinkContext.getInteger(KEY_LOG_MAX_LENGTH, DEFAULT_LOG_MAX_LENGTH);
    }

    private void reloadClientsFromSortTaskConfig(SortTaskConfig sortTaskConfig) {
        this.sinkContext = new Context(sortTaskConfig.getSinkParams());
        this.baseUrl = sinkContext.getString(KEY_BASE_URL);
        this.enableCredential = sinkContext.getBoolean(KEY_ENABLE_CREDENTIAL, DEFAULT_ENABLE_CREDENTIAL);
        this.username = sinkContext.getString(KEY_USERNAME);
        this.password = sinkContext.getString(KEY_PASSWORD);
        this.maxConnect = sinkContext.getInteger(KEY_MAX_CONNECT_TOTAL, DEFAULT_MAX_CONNECT_TOTAL);

        this.maxConnectPerRoute = sinkContext.getInteger(KEY_MAX_CONNECT_PER_ROUTE, DEFAULT_MAX_CONNECT_PER_ROUTE);
        this.connectionRequestTimeout = sinkContext.getInteger(KEY_CONNECTION_REQUEST_TIMEOUT,
                DEFAULT_CONNECTION_REQUEST_TIMEOUT);
        this.socketTimeout = sinkContext.getInteger(KEY_SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT);
        this.maxRedirects = sinkContext.getInteger(KEY_MAX_REDIRECTS, DEFAULT_MAX_REDIRECTS);
        this.logMaxLength = sinkContext.getInteger(KEY_LOG_MAX_LENGTH, DEFAULT_LOG_MAX_LENGTH);
    }

    public void addSendMetric(ProfileEvent currentRecord, String bid) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(SortMetricItem.KEY_CLUSTER_ID, this.getClusterId());
        dimensions.put(SortMetricItem.KEY_TASK_NAME, this.getTaskName());
        // metric
        fillInlongId(currentRecord, dimensions);
        dimensions.put(SortMetricItem.KEY_SINK_ID, this.getSinkName());
        dimensions.put(SortMetricItem.KEY_SINK_DATA_ID, bid);
        long msgTime = currentRecord.getRawLogTime();
        long auditFormatTime = msgTime - msgTime % CommonPropertiesHolder.getAuditFormatInterval();
        dimensions.put(SortMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        SortMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        long count = 1;
        long size = currentRecord.getBody().length;
        metricItem.sendCount.addAndGet(count);
        metricItem.sendSize.addAndGet(size);
    }

    public void addSendFailMetric() {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(SortMetricItem.KEY_CLUSTER_ID, this.getClusterId());
        dimensions.put(SortMetricItem.KEY_SINK_ID, this.getSinkName());
        long msgTime = System.currentTimeMillis();
        long auditFormatTime = msgTime - msgTime % CommonPropertiesHolder.getAuditFormatInterval();
        dimensions.put(SortMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        SortMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        metricItem.sendFailCount.incrementAndGet();
    }

    public void addSendResultMetric(ProfileEvent currentRecord, String bid, boolean result, long sendTime) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(SortMetricItem.KEY_CLUSTER_ID, this.getClusterId());
        dimensions.put(SortMetricItem.KEY_TASK_NAME, this.getTaskName());
        // metric
        fillInlongId(currentRecord, dimensions);
        dimensions.put(SortMetricItem.KEY_SINK_ID, this.getSinkName());
        dimensions.put(SortMetricItem.KEY_SINK_DATA_ID, bid);
        final long currentTime = System.currentTimeMillis();
        long msgTime = currentRecord.getRawLogTime();
        long auditFormatTime = msgTime - msgTime % CommonPropertiesHolder.getAuditFormatInterval();
        dimensions.put(SortMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        SortMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        if (result) {
            metricItem.sendSuccessCount.incrementAndGet();
            metricItem.sendSuccessSize.addAndGet(currentRecord.getBody().length);
            AuditUtils.add(AuditUtils.AUDIT_ID_SEND_SUCCESS, currentRecord);
            if (sendTime > 0) {
                long sinkDuration = currentTime - sendTime;
                long nodeDuration = currentTime - currentRecord.getFetchTime();
                long wholeDuration = currentTime - currentRecord.getRawLogTime();
                metricItem.sinkDuration.addAndGet(sinkDuration);
                metricItem.nodeDuration.addAndGet(nodeDuration);
                metricItem.wholeDuration.addAndGet(wholeDuration);
            }
        } else {
            metricItem.sendFailCount.incrementAndGet();
            metricItem.sendFailSize.addAndGet(currentRecord.getBody().length);
        }
    }

    public HttpIdConfig getIdConfig(String uid) {
        return this.idConfigMap.get(uid);
    }

    public String getNodeId() {
        return nodeId;
    }

    public Map<String, HttpIdConfig> getIdConfigMap() {
        return idConfigMap;
    }

    public Context getSinkContext() {
        return sinkContext;
    }

    public void setSinkContext(Context sinkContext) {
        this.sinkContext = sinkContext;
    }

    public DispatchProfile takeDispatchQueue() {
        DispatchProfile dispatchProfile = this.dispatchQueue.pollRecord();
        if (dispatchProfile != null) {
            this.takeCounter.incrementAndGet();
        }
        return dispatchProfile;
    }

    public void backDispatchQueue(DispatchProfile dispatchProfile) {
        this.backCounter.incrementAndGet();
        dispatchQueue.offer(dispatchProfile);
    }

    public void releaseDispatchQueue(ProfileEvent event) {
        dispatchQueue.release(event.getBody().length);
    }

    public void releaseDispatchQueue(DispatchProfile dispatchProfile) {
        dispatchQueue.release(dispatchProfile.getSize());
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public boolean getEnableCredential() {
        return enableCredential;
    }

    public void setEnableCredential(boolean enableCredential) {
        this.enableCredential = enableCredential;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMaxConnect() {
        return maxConnect;
    }

    public int getMaxConnectPerRoute() {
        return maxConnectPerRoute;
    }

    public int getConnectionRequestTimeout() {
        return connectionRequestTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public int getMaxRedirects() {
        return maxRedirects;
    }

    public int getLogMaxLength() {
        return logMaxLength;
    }

    public void setMaxConnect(int maxConnect) {
        this.maxConnect = maxConnect;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public void setIdConfigMap(Map<String, HttpIdConfig> idConfigMap) {
        this.idConfigMap = idConfigMap;
    }

    public IEvent2HttpRequestHandler createHttpRequestHandler() {
        // IEvent2HttpRequestHandler
        String httpRequestHandlerClass = CommonPropertiesHolder.getString(KEY_EVENT_HTTP_REQUEST_HANDLER,
                DefaultEvent2HttpRequestHandler.class.getName());
        try {
            Class<?> handlerClass = ClassUtils.getClass(httpRequestHandlerClass);
            Object handlerObject = handlerClass.getDeclaredConstructor().newInstance();
            if (handlerObject instanceof IEvent2HttpRequestHandler) {
                return (IEvent2HttpRequestHandler) handlerObject;
            }
        } catch (Throwable t) {
            LOG.error("Fail to init IEvent2HttpRequestHandler,handlerClass:{},error:{}",
                    httpRequestHandlerClass, t.getMessage(), t);
        }
        return null;
    }

}
