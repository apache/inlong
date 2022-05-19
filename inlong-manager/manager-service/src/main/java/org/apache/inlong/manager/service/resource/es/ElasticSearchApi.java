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

package org.apache.inlong.manager.service.resource.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.inlong.manager.common.pojo.sink.es.ElasticSearchColumnInfo;
import org.apache.inlong.manager.common.pojo.sink.es.ElasticSearchTableInfo;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * elasticsearch template service
 */
@Component
public class ElasticSearchApi {

    private static final String FIELD_KEY = "properties";

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchApi.class);

    @Autowired
    private ElasticSearchConfig esConfig;

    /**
     * Search
     *
     * @param searchRequest The search request of elasticsearch
     * @return Search reponse of elasticsearch
     * @throws IOException The io exception may throws
     */
    public SearchResponse search(SearchRequest searchRequest) throws IOException {
        return search(searchRequest, RequestOptions.DEFAULT);
    }

    /**
     * Search
     *
     * @param searchRequest The search request of elasticsearch
     * @param options The options of elasticsearch
     * @return Search reponse of elasticsearch
     * @throws IOException The io exception may throws
     */
    public SearchResponse search(SearchRequest searchRequest, RequestOptions options) throws IOException {
        LOG.info("get es search request of {}", searchRequest.source().toString());
        return getEsClient().search(searchRequest, options);
    }

    /**
     * Check index exists
     *
     * @param indexName The index name of elasticsearch
     * @return true if exists else false
     * @throws IOException The exception may throws
     */
    public boolean indexExists(String indexName) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(indexName);
        return getEsClient().indices().exists(getIndexRequest, RequestOptions.DEFAULT);
    }

    /**
     * Create index
     *
     * @param indexName The index name of elasticsearch
     * @return void
     * @throws IOException The exception may throws
     */
    public void createIndex(String indexName) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

        CreateIndexResponse createIndexResponse = getEsClient().indices()
                .create(createIndexRequest, RequestOptions.DEFAULT);
        LOG.info("create es index: {}", createIndexResponse.isAcknowledged());
    }

    /**
     * Get mapping info
     *
     * @param columnsInfo The columns info of elasticsearch
     * @return String list of columns translation
     * @throws IOException The exception may throws
     */
    private List<String> getMappingInfo(List<ElasticSearchColumnInfo> columnsInfo) {
        List<String> columnList = new ArrayList<>();
        for (ElasticSearchColumnInfo entry : columnsInfo) {
            StringBuilder columnStr = new StringBuilder().append("        \"").append(entry.getName())
                    .append("\" : {\n          \"type\" : \"")
                    .append(entry.getType()).append("\"");
            if (entry.getType().equals("text")) {
                if (StringUtils.isNotEmpty(entry.getAnalyzer())) {
                    columnStr.append(",\n          \"analyzer\" : \"")
                            .append(entry.getAnalyzer()).append("\"");
                }
                if (StringUtils.isNotEmpty(entry.getSearchAnalyzer())) {
                    columnStr.append(",\n          \"search_analyzer\" : \"")
                            .append(entry.getSearchAnalyzer()).append("\"");
                }
                if (StringUtils.isNotEmpty(entry.getIndex())) {
                    columnStr.append(",\n          \"index\" : \"")
                            .append(entry.getIndex()).append("\"");
                }
            } else if (entry.getType().equals("date")) {
                if (StringUtils.isNotEmpty(entry.getFormat())) {
                    columnStr.append(",\n          \"format\" : \"")
                            .append(entry.getFormat()).append("\"");
                }
            } else if (entry.getType().contains("float")) {
                if (StringUtils.isNotEmpty(entry.getFormat())) {
                    columnStr.append(",\n          \"scaling_factor\" : \"")
                            .append(entry.getFormat()).append("\"");
                }
            }
            columnStr.append("\n        }");
            columnList.add(columnStr.toString());
        }
        return columnList;
    }

    /**
     * Create index and mapping
     *
     * @param tableInfo Table info of creating
     * @return void
     * @throws IOException The exception may throws
     */
    public void createIndexAndMapping(ElasticSearchTableInfo tableInfo) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(tableInfo.getIndexName());
        List<String> columnList = getMappingInfo(tableInfo.getColumns());
        StringBuilder mapping = new StringBuilder().append("{\n      \"properties\" : {\n")
                .append(StringUtils.join(columnList, ",\n")).append("\n      }\n}");
        createIndexRequest.mapping(mapping.toString(), XContentType.JSON);

        CreateIndexResponse createIndexResponse = getEsClient().indices()
                .create(createIndexRequest, RequestOptions.DEFAULT);
        LOG.info("create {}:{}", tableInfo.getIndexName(), createIndexResponse.isAcknowledged());
    }

    /**
     * Get columns
     *
     * @param indexName The index name of elasticsearch
     * @return Map<String, MappingMetaData> index information
     * @throws IOException The exception may throws
     */
    public Map<String, MappingMetaData> getColumns(String indexName) throws IOException {
        GetMappingsRequest request = new GetMappingsRequest().indices(indexName);
        return getEsClient().indices().getMapping(request, RequestOptions.DEFAULT).mappings();
    }

    /**
     * Add columns
     *
     * @param indexName The index name of elasticsearch
     * @param columnInfos The columns info of elasticsearch
     * @return void
     * @throws IOException The exception may throws
     */
    public void addColumns(String indexName, List<ElasticSearchColumnInfo> columnInfos) throws IOException {
        if (CollectionUtils.isNotEmpty(columnInfos)) {
            List<String> columnList = getMappingInfo(columnInfos);
            StringBuilder mapping = new StringBuilder().append("{\n      \"properties\" : {\n")
                    .append(StringUtils.join(columnList, ",\n")).append("\n      }\n}");
            System.out.println(mapping.toString());
            PutMappingRequest indexRequest = new PutMappingRequest(indexName)
                    .source(mapping.toString(), XContentType.JSON);
            AcknowledgedResponse acknowledgedResponse = getEsClient().indices()
                    .putMapping(indexRequest, RequestOptions.DEFAULT);
            LOG.info("put mapping: {} result: {}", mapping.toString(), acknowledgedResponse.toString());
        }
    }

    /**
     * Add not exist columns
     *
     * @param indexName The index name of elasticsearch
     * @param columnInfos The columns info of elasticsearch
     * @return void
     * @throws IOException The exception may throws
     */
    public void addNotExistColumns(String indexName,
            List<ElasticSearchColumnInfo> columnInfos) throws IOException {
        List<ElasticSearchColumnInfo> notExistColumnInfos = new ArrayList<>(columnInfos);
        Map<String, MappingMetaData> mapping = getColumns(indexName);
        Map<String, Object> filedMap = (Map<String, Object>)mapping.get(indexName).getSourceAsMap().get(FIELD_KEY);
        for (String key : filedMap.keySet()) {
            for (ElasticSearchColumnInfo entry : notExistColumnInfos) {
                if (entry.getName().equals(key)) {
                    notExistColumnInfos.remove(entry);
                    break;
                }
            }
        }
        addColumns(indexName, notExistColumnInfos);
    }

    /**
     * Get ElasticSearch client
     *
     * @return RestHighLevelClient
     */
    private RestHighLevelClient getEsClient() {
        return esConfig.highLevelClient();
    }

    /**
     * Get ElasticSearch client
     *
     * @param config ElasticSearch's configuration
     * @return void
     */
    public void setEsConfig(ElasticSearchConfig config) {
        this.esConfig = config;
    }
}
