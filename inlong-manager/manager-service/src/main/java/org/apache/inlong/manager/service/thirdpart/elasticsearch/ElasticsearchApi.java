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
