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

package org.apache.inlong.audit.store.service;

import org.apache.inlong.audit.consts.ConfigConstants;
import org.apache.inlong.audit.file.RemoteConfigJson;
import org.apache.inlong.audit.store.config.JdbcConfig;
import org.apache.inlong.audit.store.config.MessageQueueConfig;
import org.apache.inlong.audit.store.config.StoreConfig;
import org.apache.inlong.audit.store.metric.MetricsManager;
import org.apache.inlong.audit.store.route.AuditRouteManager;
import org.apache.inlong.audit.store.service.consume.BaseConsume;
import org.apache.inlong.audit.store.service.consume.KafkaConsume;
import org.apache.inlong.audit.store.service.consume.PulsarConsume;
import org.apache.inlong.audit.store.service.consume.TubeConsume;
import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.common.pojo.audit.AuditConfigRequest;
import org.apache.inlong.common.pojo.audit.MQInfo;

import com.google.gson.Gson;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Service
public class AuditMsgConsumerServer implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(AuditMsgConsumerServer.class);
    @Autowired
    private MessageQueueConfig mqConfig;
    @Autowired
    private StoreConfig storeConfig;
    @Autowired
    private JdbcConfig jdbcConfig;
    private JdbcService jdbcService;
    private static final String DEFAULT_CONFIG_PROPERTIES = "application.properties";
    // interval time of getting mq config
    private static final int INTERVAL_MS = 5000;
    private final CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    private final Gson gson = new Gson();
    /**
     * Initializing bean
     */
    public void afterPropertiesSet() {
        AuditRouteManager.getInstance().init(jdbcConfig.getUrl());

        List<MQInfo> mqInfoList = getClusterFromManager();
        BaseConsume mqConsume = null;
        List<InsertData> insertServiceList = this.getInsertServiceList();

        for (MQInfo mqInfo : mqInfoList) {
            if (mqConfig.isPulsar() && MQType.PULSAR.equals(mqInfo.getMqType())) {
                mqConfig.setPulsarServerUrl(mqInfo.getUrl());
                mqConsume = new PulsarConsume(insertServiceList, storeConfig, mqConfig);
                break;
            } else if (mqConfig.isTube() && MQType.TUBEMQ.equals(mqInfo.getMqType())) {
                mqConfig.setTubeMasterList(mqInfo.getUrl());
                mqConsume = new TubeConsume(insertServiceList, storeConfig, mqConfig);
                break;
            } else if (mqConfig.isKafka() && MQType.KAFKA.equals(mqInfo.getMqType())) {
                mqConfig.setKafkaServerUrl(mqInfo.getUrl());
                mqConsume = new KafkaConsume(insertServiceList, storeConfig, mqConfig);
                break;
            }
        }

        if (mqConsume == null) {
            LOG.error("Unknown MessageQueue {}", mqConfig.getMqType());
        }
        if (storeConfig.isJdbc()) {
            jdbcService.start();
        }
        if (mqConsume != null) {
            mqConsume.start();
        }

        MetricsManager.getInstance().init();
    }

    /**
     * getInsertServiceList
     *
     * @return
     */
    private List<InsertData> getInsertServiceList() {
        List<InsertData> insertServiceList = new ArrayList<>();
        if (storeConfig.isJdbc()) {
            // create jdbc object
            jdbcService = new JdbcService(jdbcConfig);
            insertServiceList.add(jdbcService);
        }
        return insertServiceList;
    }

    private List<MQInfo> getClusterFromManager() {
        Properties properties = new Properties();
        List<MQInfo> mqConfig;
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(DEFAULT_CONFIG_PROPERTIES)) {
            properties.load(inputStream);
            String managerHosts = properties.getProperty("manager.hosts");
            String clusterTag = properties.getProperty("default.mq.cluster.tag");
            String[] hostList = StringUtils.split(managerHosts, ",");
            for (String host : hostList) {
                while (true) {
                    mqConfig = getMQConfig(host, clusterTag);
                    if (ObjectUtils.isNotEmpty(mqConfig)) {
                        return mqConfig;
                    }
                    LOG.info("MQ config may not be registered yet, wait for 5s and try again");
                    Thread.sleep(INTERVAL_MS);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private List<MQInfo> getMQConfig(String host, String clusterTag) {
        HttpPost httpPost = null;
        try {
            String url = "http://" + host + ConfigConstants.MANAGER_PATH + ConfigConstants.MANAGER_GET_CONFIG_PATH;
            LOG.info("start to request {} to get config info", url);
            httpPost = new HttpPost(url);
            httpPost.addHeader(HttpHeaders.CONNECTION, "close");

            // request body
            AuditConfigRequest request = new AuditConfigRequest();
            request.setClusterTag(clusterTag);
            StringEntity stringEntity = new StringEntity(gson.toJson(request));
            stringEntity.setContentType("application/json");
            httpPost.setEntity(stringEntity);

            // request with post
            LOG.info("start to request {} to get config info with params {}", url, request);
            CloseableHttpResponse response = httpClient.execute(httpPost);
            String returnStr = EntityUtils.toString(response.getEntity());
            RemoteConfigJson configJson = gson.fromJson(returnStr, RemoteConfigJson.class);
            if (configJson.isSuccess() && configJson.getData() != null) {
                List<MQInfo> mqInfoList = configJson.getData().getMqInfoList();
                if (mqInfoList != null && !mqInfoList.isEmpty()) {
                    return mqInfoList;
                }
            }
        } catch (Exception ex) {
            LOG.error("Failed to get MQ config from manager, please check it", ex);
            return null;
        } finally {
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
        }
        return null;
    }

    @PreDestroy
    public void shutdown() {
        MetricsManager.getInstance().shutdown();
        AuditRouteManager.getInstance().shutdown();
    }
}
