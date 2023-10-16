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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

/**
 * Elasticsearch config information, including host, port, etc.
 */
@Data
@Component
public class ElasticsearchConfig {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConfig.class);
    private static RestTemplate restTemplate;
    private static List<HttpHost> httpHosts;
    @Value("${es.index.search.hostname}")
    private String hosts;
    @Value("${es.auth.enable}")
    private Boolean authEnable = false;
    @Value("${es.auth.user}")
    private String username;
    @Value("${es.auth.password}")
    private String password;

    /**
     * highLevelClient
     *
     * @return RestHighLevelClient
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

    public String getOneHttpUrl() throws Exception {
        getHttpHosts();
        if (!httpHosts.isEmpty() && httpHosts.size() > 0) {
            return httpHosts.get(0).toString();
        } else {
            throw new Exception("http hosts is empty! please check hosts!");
        }
    }
}
