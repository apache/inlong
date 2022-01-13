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


package org.apache.inlong.manager.service.thirdpart.elasticsearch;

import java.io.IOException;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * elasticsearch template service
 */
@Component
public class ElasticsearchApi {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchApi.class);

    @Autowired
    private ElasticsearchConfig esConfig;

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

    private RestHighLevelClient getEsClient() {
        return esConfig.highLevelClient();
    }
}
