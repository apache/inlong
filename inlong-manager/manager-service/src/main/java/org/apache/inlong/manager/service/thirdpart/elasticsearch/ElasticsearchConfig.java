package org.apache.inlong.manager.service.thirdpart.elasticsearch;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ElasticsearchConfig {

    @Value("${es.index.search.hostname}")
    private String host;
    @Value("${es.index.search.port}")
    private Integer port = 9200;
    @Value("${es.auth.enable}")
    private Boolean authEnable = false;
    @Value("${es.auth.user}")
    private String user;
    @Value("${es.auth.password}")
    private String password;

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConfig.class);

    private static RestHighLevelClient highLevelClient;


    public RestHighLevelClient highLevelClient() {
        if (highLevelClient != null) {
            return highLevelClient;
        }
        try {
            synchronized (RestHighLevelClient.class) {
                if (highLevelClient == null) {
                    List<HttpHost> hosts = new ArrayList<>();
                    String[] hostArrays = host.split(",");
                    for (String host : hostArrays) {
                        if (StringUtils.isNotBlank(host)) {
                            host = host.trim();
                            hosts.add(new HttpHost(host, port, "http"));
                        }
                    }
                    RestClientBuilder clientBuilder = RestClient.builder(hosts.toArray(new HttpHost[0]));
                    this.setEsAuth(clientBuilder);
                    highLevelClient = new RestHighLevelClient(clientBuilder);
                }
            }
        } catch (Exception e) {
            logger.error("get es high level client error", e);
        }
        return highLevelClient;
    }

    /**
     * Elasticsearch authentication
     *
     * @param builder The builder
     */
    private void setEsAuth(RestClientBuilder builder) {
        try {
            logger.info("set es auth of enable={}", authEnable);
            if (authEnable) {
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
                builder.setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

            }
        } catch (Exception e) {
            logger.error("set es auth error ", e);
        }
    }
}
