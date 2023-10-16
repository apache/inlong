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

import org.apache.inlong.manager.common.util.HttpUtils;
import org.apache.inlong.manager.pojo.sink.es.ElasticsearchCreateIndexResponse;
import org.apache.inlong.manager.pojo.sink.es.ElasticsearchFieldInfo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException.NotFound;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * elasticsearch template service
 */
@Slf4j
@Component
public class ElasticsearchApi {

    private static final Gson GSON = new GsonBuilder().create();

    private static final String MAPPINGS_KEY = "mappings";

    private static final String FIELD_KEY = "properties";

    private static final String CONTENT_TYPE_KEY = "Content-Type";

    private static final String CONTENT_TYPE_VALUE = "application/json;charset=UTF-8";

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchApi.class);

    @Autowired
    private ElasticsearchConfig esConfig;

    /**
     * get http headers by token.
     *
     * @return
     */
    private HttpHeaders getHttpHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.add(CONTENT_TYPE_KEY, CONTENT_TYPE_VALUE);
        if (esConfig.getAuthEnable()) {
            if (StringUtils.isNotEmpty(esConfig.getUsername()) && StringUtils.isNotEmpty(esConfig.getPassword())) {
                String tokenStr = esConfig.getUsername() + ":" + esConfig.getPassword();
                String token = String.valueOf(new BASE64Encoder().encode(tokenStr.getBytes(StandardCharsets.UTF_8)));
                headers.add("Authorization", "Basic " + token);
            }
        }
        return headers;
    }

    /**
     * Search
     *
     * @param indexName The index name
     * @param request The request json
     * @return
     * @throws Exception
     */
    public JsonObject search(String indexName, JsonObject request) throws Exception {
        LOG.info("get es search es index:{} request:{}", indexName, request.toString());
        String url = esConfig.getOneHttpUrl() + "/" + indexName + "/_search";
        return HttpUtils.request(esConfig.getRestClient(), url, HttpMethod.POST, request.toString(), getHttpHeaders(),
                JsonObject.class);
    }

    /**
     * Check index exists
     *
     * @param indexName
     * @return
     * @throws Exception
     */
    public boolean indexExists(String indexName) throws Exception {
        String url = esConfig.getOneHttpUrl() + "/" + indexName;
        HttpHeaders header = new HttpHeaders();
        header.add(CONTENT_TYPE_KEY, CONTENT_TYPE_VALUE);
        try {
            return HttpUtils.headRequest(esConfig.getRestClient(), url, null, header);
        } catch (NotFound e) {
            return false;
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Check if the cluster is available
     *
     * @return
     */
    public boolean ping() throws Exception {
        String url = esConfig.getOneHttpUrl() + "/";
        return HttpUtils.headRequest(esConfig.getRestClient(), url, null, getHttpHeaders());
    }

    /**
     * Create index by REST API
     *
     * @param indexName
     * @throws IOException
     */
    public void createIndex(String indexName) throws Exception {
        String url = esConfig.getOneHttpUrl() + "/" + indexName;
        ElasticsearchCreateIndexResponse response = HttpUtils.putRequest(esConfig.getRestClient(), url, null,
                getHttpHeaders(), new ParameterizedTypeReference<ElasticsearchCreateIndexResponse>() {
                });
        LOG.info("create es index:{} result: {}", indexName, response.getAcknowledged());
    }

    /**
     * Get mapping info
     *
     * @param fieldsInfo The fields info of Elasticsearch
     * @return String list of fields translation
     * @throws IOException The exception may throws
     */
    private List<String> getMappingInfo(List<ElasticsearchFieldInfo> fieldsInfo) {
        List<String> fieldList = new ArrayList<>();
        for (ElasticsearchFieldInfo field : fieldsInfo) {
            StringBuilder fieldStr = new StringBuilder().append("        \"").append(field.getFieldName())
                    .append("\" : {\n          \"type\" : \"")
                    .append(field.getFieldType()).append("\"");
            if (field.getFieldType().equals("text")) {
                if (StringUtils.isNotEmpty(field.getAnalyzer())) {
                    fieldStr.append(",\n          \"analyzer\" : \"")
                            .append(field.getAnalyzer()).append("\"");
                }
                if (StringUtils.isNotEmpty(field.getSearchAnalyzer())) {
                    fieldStr.append(",\n          \"search_analyzer\" : \"")
                            .append(field.getSearchAnalyzer()).append("\"");
                }
            } else if (field.getFieldType().equals("date")) {
                if (StringUtils.isNotEmpty(field.getFieldFormat())) {
                    fieldStr.append(",\n          \"format\" : \"")
                            .append(field.getFieldFormat()).append("\"");
                }
            } else if (field.getFieldType().equals("scaled_float")) {
                if (StringUtils.isNotEmpty(field.getScalingFactor())) {
                    fieldStr.append(",\n          \"scaling_factor\" : \"")
                            .append(field.getScalingFactor()).append("\"");
                }
            }
            fieldStr.append("\n        }");
            fieldList.add(fieldStr.toString());
        }
        return fieldList;
    }

    /**
     * Create index and mapping by REST API
     *
     * @param indexName Index name of creating
     * @param fieldInfos Field infos
     * @throws Exception The exception may throws
     */
    public void createIndexAndMapping(String indexName, List<ElasticsearchFieldInfo> fieldInfos) throws Exception {
        List<String> fieldList = getMappingInfo(fieldInfos);
        StringBuilder mapping = new StringBuilder().append("\n{\"mappings\":\n{\n\"properties\":{\n")
                .append(StringUtils.join(fieldList, ",\n")).append("\n}\n}\n}");
        String url = esConfig.getOneHttpUrl() + "/" + indexName;
        ElasticsearchCreateIndexResponse response = HttpUtils.request(esConfig.getRestClient(), url, HttpMethod.PUT,
                mapping.toString(), getHttpHeaders(),
                new ParameterizedTypeReference<ElasticsearchCreateIndexResponse>() {
                });
        LOG.info("create {}:{}", indexName, response.getIndex());
    }

    /**
     * Get mapping map
     *
     * @param indexName
     * @return
     */
    public Map<String, Map<String, String>> getMappingMap(String indexName) throws Exception {
        String url = esConfig.getOneHttpUrl() + "/" + indexName + "/_mapping";
        JsonObject result = HttpUtils.request(esConfig.getRestClient(), url, HttpMethod.GET, null, getHttpHeaders(),
                JsonObject.class);
        JsonObject mappings = result.getAsJsonObject(indexName);
        JsonObject properties = null;
        JsonObject fields = null;
        Map<String, Map<String, String>> fieldInfo = null;
        if (ObjectUtils.isNotEmpty(mappings)) {
            properties = mappings.getAsJsonObject(MAPPINGS_KEY);
        }
        if (ObjectUtils.isNotEmpty(properties)) {
            fields = properties.getAsJsonObject(FIELD_KEY);
        }
        if (ObjectUtils.isNotEmpty(fields)) {
            fieldInfo = GSON.fromJson(fields.toString(), Map.class);
        }
        return fieldInfo;
    }

    /**
     * Add fields by REST API
     *
     * @param indexName
     * @param fieldInfos
     * @throws Exception
     */
    public void addFields(String indexName, List<ElasticsearchFieldInfo> fieldInfos) throws Exception {
        List<String> fieldList = getMappingInfo(fieldInfos);
        if (!fieldList.isEmpty()) {
            String url = esConfig.getOneHttpUrl() + "/" + indexName + "/_mapping";
            StringBuilder mapping = new StringBuilder().append("{\n\"properties\":{\n")
                    .append(StringUtils.join(fieldList, ",\n")).append("\n}\n}");
            HttpUtils.request(esConfig.getRestClient(), url, HttpMethod.PUT,
                    mapping.toString(), getHttpHeaders(), Object.class);
        }
    }

    /**
     * Add not exist fields by REST API
     *
     * @param indexName
     * @param fieldInfos
     * @throws Exception
     */
    public void addNotExistFields(String indexName, List<ElasticsearchFieldInfo> fieldInfos) throws Exception {
        List<ElasticsearchFieldInfo> notExistFieldInfos = new ArrayList<>();
        Map<String, Map<String, String>> mappings = getMappingMap(indexName);
        for (ElasticsearchFieldInfo fieldInfo : fieldInfos) {
            if (!mappings.containsKey(fieldInfo.getFieldName())) {
                notExistFieldInfos.add(fieldInfo);
            }
        }
        if (!notExistFieldInfos.isEmpty()) {
            addFields(indexName, notExistFieldInfos);
        }
    }

    /**
     * Get Elasticsearch client
     *
     * @param config Elasticsearch's configuration
     */
    public void setEsConfig(ElasticsearchConfig config) {
        this.esConfig = config;
    }
}
