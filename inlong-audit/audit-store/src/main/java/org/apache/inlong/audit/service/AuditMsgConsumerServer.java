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

package org.apache.inlong.audit.service;

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
import org.apache.inlong.audit.config.ClickHouseConfig;
import org.apache.inlong.audit.config.MessageQueueConfig;
import org.apache.inlong.audit.config.StoreConfig;
import org.apache.inlong.audit.db.dao.AuditDataDao;
import org.apache.inlong.audit.file.RemoteConfigJson;
import org.apache.inlong.audit.service.consume.BaseConsume;
import org.apache.inlong.audit.service.consume.KafkaConsume;
import org.apache.inlong.audit.service.consume.PulsarConsume;
import org.apache.inlong.audit.service.consume.TubeConsume;
import org.apache.inlong.common.pojo.dataproxy.DataProxyConfigRequest;
import org.apache.inlong.common.pojo.dataproxy.MQClusterInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
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
    private AuditDataDao auditDataDao;
    @Autowired
    private ElasticsearchService esService;
    @Autowired
    private StoreConfig storeConfig;
    @Autowired
    private ClickHouseConfig chConfig;
    // ClickHouseService
    private ClickHouseService ckService;

    private static final String DEFAULT_CONFIG_PROPERTIES = "server.properties";

    private final CloseableHttpClient httpClient = HttpClientBuilder.create().build();

    /**
     * Initializing bean
     */
    public void afterPropertiesSet() {
        List<MQClusterInfo> mqConfigList = getConfigManager();
        BaseConsume mqConsume = null;
        List<InsertData> insertServiceList = this.getInsertServiceList();

        for (MQClusterInfo mqClusterInfo : mqConfigList) {
            if (mqConfig.isPulsar()) {
                mqConfig.setPulsarServerUrl(mqClusterInfo.getUrl());
                mqConsume = new PulsarConsume(insertServiceList, storeConfig, mqConfig);
                break;
            } else if (mqConfig.isTube()) {
                mqConfig.setTubeMasterList(mqClusterInfo.getUrl());
                mqConsume = new TubeConsume(insertServiceList, storeConfig, mqConfig);
                break;
            } else if (mqConfig.isKafka()) {
                mqConfig.setKafkaServerUrl(mqClusterInfo.getUrl());
                mqConsume = new KafkaConsume(insertServiceList, storeConfig, mqConfig);
                break;
            } else {
                LOG.error("Unknown MessageQueue {}", mqConfig.getMqType());
                return;
            }
        }

        if (storeConfig.isElasticsearchStore()) {
            esService.startTimerRoutine();
        }
        if (storeConfig.isClickHouseStore()) {
            ckService.start();
        }
        mqConsume.start();
    }

    /**
     * getInsertServiceList
     *
     * @return
     */
    private List<InsertData> getInsertServiceList() {
        List<InsertData> insertServiceList = new ArrayList<>();
        if (storeConfig.isMysqlStore()) {
            insertServiceList.add(new MySqlService(auditDataDao));
        }
        if (storeConfig.isElasticsearchStore()) {
            insertServiceList.add(esService);
        }
        if (storeConfig.isClickHouseStore()) {
            // create ck object
            ckService = new ClickHouseService(chConfig);
            insertServiceList.add(ckService);
        }
        return insertServiceList;
    }

    private List<MQClusterInfo> getConfigManager() {
        Properties properties = new Properties();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(DEFAULT_CONFIG_PROPERTIES)) {
            properties.load(inputStream);
            String managerHosts = properties.getProperty("manager.hosts");
            String clusterName = properties.getProperty("proxy.cluster.name");
            String clusterTag = properties.getProperty("proxy.cluster.tag");
            String[] hostList = StringUtils.split(managerHosts, ",");
            for (String host : hostList) {
                List<MQClusterInfo> mqConfig = getMQConfig(host, clusterName, clusterTag);
                if (ObjectUtils.isNotEmpty(mqConfig)) {
                    return mqConfig;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private List<MQClusterInfo> getMQConfig(String host, String clusterName, String clusterTag) {
        HttpPost httpPost = null;
        Gson gson = new Gson();
        try {
            String url = "http://" + host + "/inlong/manager/openapi/dataproxy/getConfig";
            LOG.info("start to request {} to get config info", url);
            httpPost = new HttpPost(url);
            httpPost.addHeader(HttpHeaders.CONNECTION, "close");

            // request body
            DataProxyConfigRequest request = new DataProxyConfigRequest();
            request.setClusterName(clusterName);
            request.setClusterTag(clusterTag);
            StringEntity stringEntity = new StringEntity(gson.toJson(request));
            stringEntity.setContentType("application/json");
            httpPost.setEntity(stringEntity);

            // request with post
            LOG.info("start to request {} to get config info with params {}", url, request);
            CloseableHttpResponse response = httpClient.execute(httpPost);
            String returnStr = EntityUtils.toString(response.getEntity());
            // get groupId <-> topic and m value.

            RemoteConfigJson configJson = gson.fromJson(returnStr, RemoteConfigJson.class);
            if (configJson.isSuccess() && configJson.getData() != null) {
                LOG.info("getConfig result: {}", configJson);
                List<MQClusterInfo> mqClusterInfoList = configJson.getData().getMqClusterList();
                if (mqClusterInfoList != null && !mqClusterInfoList.isEmpty()) {
                    return mqClusterInfoList;
                }
            }
        } catch (Exception ex) {
            LOG.error("exception caught", ex);
            return null;
        } finally {
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
        }
        return null;
    }
}
