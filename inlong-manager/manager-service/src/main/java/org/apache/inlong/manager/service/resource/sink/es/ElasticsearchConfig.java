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

package org.apache.inlong.manager.service.resource.sink.es;

import org.apache.inlong.manager.common.consts.InlongConstants;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Elasticsearch config information, including host, port, etc.
 */
@Data
@Component
public class ElasticsearchConfig {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConfig.class);

    private final Random rand = new Random();

    @Autowired
    private RestTemplate restTemplate;

    private List<HttpHost> httpHosts;
    private String hosts;
    private Boolean authEnable = false;
    private String username;
    private String password;

    /**
     *  Get http rest client.
     *
     * @return springframework RestTemplate
     */
    public RestTemplate getRestClient() {
        if (restTemplate != null) {
            return restTemplate;
        }
        try {
            synchronized (RestTemplate.class) {
                if (restTemplate == null) {
                    restTemplate = new RestTemplate();
                }
            }
        } catch (Exception e) {
            logger.error("get es high level client error", e);
        }
        return restTemplate;
    }

    /**
     * Get http hosts.
     *
     * @return list of http host info
     */
    public List<HttpHost> getHttpHosts() {
        if (httpHosts != null) {
            return httpHosts;
        }
        try {
            synchronized (HttpHost.class) {
                httpHosts = new ArrayList<>();
                String[] hostArrays = this.hosts.split(InlongConstants.SEMICOLON);
                for (String host : hostArrays) {
                    if (StringUtils.isNotBlank(host)) {
                        host = host.trim();
                        httpHosts.add(HttpHost.create(host));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("get es http hosts error", e);
        }
        return httpHosts;
    }

    /**
     * Get one http url.
     *
     * @return a http url
     * @throws Exception any exception if occurred
     */
    public String getOneHttpUrl() throws Exception {
        getHttpHosts();
        if (!httpHosts.isEmpty() && httpHosts.size() > 0) {
            return httpHosts.get(rand.nextInt(httpHosts.size())).toString();
        }
        throw new Exception("http hosts is empty! please check hosts!");
    }

    /**
     * Get all http url.
     *
     * @param urlSuffix
     * @return http url array
     * @throws Exception
     */
    public String[] getHttpUrls(String urlSuffix) throws Exception {
        getHttpHosts();
        if (!httpHosts.isEmpty() && httpHosts.size() > 0) {
            String[] urls = new String[httpHosts.size()];
            for (int i = 0; i < urls.length; i++) {
                urls[i] = httpHosts.get(i) + urlSuffix;
            }
            return urls;
        }
        throw new Exception("http hosts is empty! please check hosts!");
    }
}
