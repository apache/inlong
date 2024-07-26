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
import org.apache.inlong.manager.common.util.HttpUtils;
import org.apache.inlong.manager.pojo.sink.es.ElasticsearchCreateIndexResponse;
import org.apache.inlong.manager.pojo.sink.es.ElasticsearchFieldInfo;
import org.apache.inlong.manager.pojo.sink.es.ElasticsearchIndexMappingInfo;
import org.apache.inlong.manager.pojo.sink.es.ElasticsearchIndexMappingInfo.IndexField;
import org.apache.inlong.manager.pojo.sink.es.ElasticsearchIndexMappingInfo.IndexMappings;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException.NotFound;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.Map;

/**
 * elasticsearch template service
 */
@Slf4j
@Component
public class ElasticsearchApi {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchApi.class);

    private static final Gson GSON = new GsonBuilder().create();

    private static final String MAPPINGS_KEY = "mappings";

    private static final String FIELD_KEY = "properties";

    private static final String CONTENT_TYPE_KEY = "Content-Type";

    private static final String FIELD_TYPE = "type";

    private static final String FIELD_FORMAT = "format";

    private static final String CONTENT_TYPE_VALUE = "application/json;charset=UTF-8";

    private final Encoder base64Encoder = Base64.getEncoder();

    @Autowired
    private ElasticsearchConfig esConfig;

    /**
     * Get http headers by token.
     *
     * @return http header infos
     */
    private HttpHeaders getHttpHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.add(CONTENT_TYPE_KEY, CONTENT_TYPE_VALUE);
        if (esConfig.getAuthEnable()) {
            if (StringUtils.isNotEmpty(esConfig.getUsername()) && StringUtils.isNotEmpty(esConfig.getPassword())) {
                String tokenStr = esConfig.getUsername() + ":" + esConfig.getPassword();
                String token = base64Encoder.encodeToString(tokenStr.getBytes(StandardCharsets.UTF_8));
                headers.add("Authorization", "Basic " + token);
            }
        }
        return headers;
    }

    /**
     * Search.
     *
     * @param indexName The index name
     * @param request The request json
     * @return the elasticsearch seqrch result
     * @throws Exception any exception if occurred
     */
    public JsonObject search(String indexName, JsonObject request) throws Exception {
        LOG.info("get es search es index:{} request:{}", indexName, request.toString());
        final String url = esConfig.getOneHttpUrl() + InlongConstants.SLASH + indexName + "/_search";
        return HttpUtils.request(esConfig.getRestClient(), url, HttpMethod.POST, request.toString(), getHttpHeaders(),
                JsonObject.class);
    }

    /**
     * Check index exists.
     *
     * @param indexName The elasticsearch index name
     * @return true or false
     * @throws Exception any exception if occurred
     */
    public boolean indexExists(String indexName) throws Exception {
        final String url = esConfig.getOneHttpUrl() + InlongConstants.SLASH + indexName;
        try {
            return HttpUtils.headRequest(esConfig.getRestClient(), url, null, getHttpHeaders());
        } catch (NotFound e) {
            return false;
        }
    }

    /**
     * Check if the cluster is available.
     *
     * @return true or false
     */
    public boolean ping() throws Exception {
        final String[] urls = esConfig.getHttpUrls(InlongConstants.SLASH);
        boolean result = true;
        for (String url : urls) {
            result &= HttpUtils.headRequest(esConfig.getRestClient(), url, null, getHttpHeaders());
        }
        return result;
    }

    /**
     * Create index by REST API.
     *
     * @param indexName elasticsearch index name
     * @throws IOException any IOException if occurred
     */
    public void createIndex(String indexName) throws Exception {
        final String url = esConfig.getOneHttpUrl() + InlongConstants.SLASH + indexName;
        ElasticsearchCreateIndexResponse response = HttpUtils.request(esConfig.getRestClient(), url, HttpMethod.PUT,
                null, getHttpHeaders(), ElasticsearchCreateIndexResponse.class);
        LOG.info("create es index:{} result: {}", indexName, response.getAcknowledged());
    }

    /**
     * Get mapping info.
     *
     * @param fieldsInfo The fields info of Elasticsearch
     * @return elasticsearch index mappings object {@link ElasticsearchIndexMappingInfo}
     * @throws IOException The exception may throws
     */
    private ElasticsearchIndexMappingInfo getMappingInfo(List<ElasticsearchFieldInfo> fieldsInfo) {
        Map<String, IndexField> fields = Maps.newHashMap();
        for (ElasticsearchFieldInfo field : fieldsInfo) {
            IndexField indexField = new IndexField();
            fields.put(field.getFieldName(), indexField);
            indexField.setType(field.getFieldType());
            if (field.getFieldType().equals("text")) {
                if (StringUtils.isNotEmpty(field.getAnalyzer())) {
                    indexField.setAnalyzer(field.getAnalyzer());
                }
                if (StringUtils.isNotEmpty(field.getSearchAnalyzer())) {
                    indexField.setSearchAnalyzer(field.getSearchAnalyzer());
                }
            } else if (field.getFieldType().equals("date")) {
                if (StringUtils.isNotEmpty(field.getFieldFormat())) {
                    indexField.setFormat(field.getFieldFormat());
                }
            } else if (field.getFieldType().equals("scaled_float")) {
                if (StringUtils.isNotEmpty(field.getScalingFactor())) {
                    indexField.setScalingFactor(field.getScalingFactor());
                }
            }
        }
        return ElasticsearchIndexMappingInfo.builder().mappings(IndexMappings.builder()
                .properties(fields).build()).build();
    }

    /**
     * Create index and mapping by REST API.
     *
     * @param indexName Index name of creating
     * @param fieldInfos Field infos
     * @throws Exception The exception may throws
     */
    public void createIndexAndMapping(String indexName, List<ElasticsearchFieldInfo> fieldInfos) throws Exception {
        ElasticsearchIndexMappingInfo mappingInfo = getMappingInfo(fieldInfos);
        final String url = esConfig.getOneHttpUrl() + "/" + indexName;
        ElasticsearchCreateIndexResponse response = HttpUtils.request(esConfig.getRestClient(), url, HttpMethod.PUT,
                GSON.toJsonTree(mappingInfo).getAsJsonObject().toString(), getHttpHeaders(),
                ElasticsearchCreateIndexResponse.class);
        LOG.info("create {}:{}", indexName, response.getIndex());
    }

    /**
     * Get mapping map.
     *
     * @param indexName elasticsearch index name
     * @return map of elasticsearch index mapping info
     */
    public Map<String, ElasticsearchFieldInfo> getMappingMap(String indexName) throws Exception {
        final String url = esConfig.getOneHttpUrl() + InlongConstants.SLASH + indexName + "/_mapping";
        JsonObject result = HttpUtils.request(esConfig.getRestClient(), url, HttpMethod.GET, null, getHttpHeaders(),
                JsonObject.class);
        JsonObject mappings = result.getAsJsonObject(indexName);
        JsonObject properties = null;
        JsonObject fields = null;
        Map<String, ElasticsearchFieldInfo> fieldInfos = Maps.newHashMap();
        if (ObjectUtils.isNotEmpty(mappings)) {
            String MAPPINGS_KEY = "mappings";
            properties = mappings.getAsJsonObject(MAPPINGS_KEY);
        }
        if (ObjectUtils.isNotEmpty(properties)) {
            fields = properties.getAsJsonObject(FIELD_KEY);
        }
        if (ObjectUtils.isNotEmpty(fields)) {
            for (String key : fields.keySet()) {
                JsonObject field = fields.getAsJsonObject(key);
                if (StringUtils.isNotEmpty(key) && ObjectUtils.isNotEmpty(field)) {
                    ElasticsearchFieldInfo fieldInfo = new ElasticsearchFieldInfo();
                    if (ObjectUtils.isNotEmpty(field.get(FIELD_TYPE))) {
                        fieldInfo.setFieldType(field.get(FIELD_TYPE).getAsString());
                    }
                    if (ObjectUtils.isNotEmpty(field.get(FIELD_FORMAT))) {
                        fieldInfo.setFieldFormat(field.get(FIELD_FORMAT).getAsString());
                    }
                    fieldInfo.setFieldName(key);
                    fieldInfos.put(key, fieldInfo);
                }
            }
        }
        return fieldInfos;
    }

    /**
     * Add fields by REST API.
     *
     * @param indexName elasticsearch index name
     * @param fieldInfos elasticsearch field infos
     * @throws Exception any exception if occurred
     */
    public void addFields(String indexName, List<ElasticsearchFieldInfo> fieldInfos) throws Exception {
        ElasticsearchIndexMappingInfo mappingInfo = getMappingInfo(fieldInfos);
        if (ObjectUtils.isNotEmpty(mappingInfo) && !mappingInfo.getMappings().getProperties().isEmpty()) {
            String url = esConfig.getOneHttpUrl() + InlongConstants.SLASH + indexName + "/_mapping";
            HttpUtils.request(esConfig.getRestClient(), url, HttpMethod.PUT,
                    GSON.toJsonTree(mappingInfo.getMappings()).getAsJsonObject().toString(), getHttpHeaders(),
                    Object.class);
        }
    }

    /**
     * Add not exist fields by REST API.
     *
     * @param indexName elasticsearch index name
     * @param fieldInfos elasticsearch field infos
     * @throws Exception any exception if occurred
     */
    public void addNotExistFields(String indexName, List<ElasticsearchFieldInfo> fieldInfos) throws Exception {
        List<ElasticsearchFieldInfo> notExistFieldInfos = new ArrayList<>();
        Map<String, ElasticsearchFieldInfo> mappings = getMappingMap(indexName);
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
     * Get Elasticsearch configuration.
     *
     * @param config Elasticsearch's configuration
     */
    public void setEsConfig(ElasticsearchConfig config) {
        this.esConfig = config;
    }
}
