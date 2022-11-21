package org.apache.inlong.sort.elasticsearch5;

import org.apache.flink.util.InstantiationUtil;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

/** A resource that starts an embedded elasticsearch cluster. */
public class ElasticsearchResource extends ExternalResource {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchResource.class);
    private EmbeddedElasticsearchNodeEnvironment embeddedNodeEnv;
    private final TemporaryFolder tempFolder = new TemporaryFolder();

    private final String clusterName;

    public ElasticsearchResource(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    protected void before() throws Throwable {

        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    Starting embedded Elasticsearch node ");
        LOG.info("-------------------------------------------------------------------------");

        // dynamically load version-specific implementation of the Elasticsearch embedded node
        // environment
        Class<?> clazz =
                Class.forName(
                        "org.apache.flink.streaming.connectors.elasticsearch.EmbeddedElasticsearchNodeEnvironmentImpl");
        embeddedNodeEnv =
                (EmbeddedElasticsearchNodeEnvironment) InstantiationUtil.instantiate(clazz);

        tempFolder.create();
        embeddedNodeEnv.start(tempFolder.newFolder(), clusterName);

        waitForCluster();
    }

    /** Blocks until the cluster is ready and data nodes/nodes are live. */
    private void waitForCluster() {
        AdminClient adminClient = embeddedNodeEnv.getClient().admin();
        ClusterAdminClient clusterAdminClient = adminClient.cluster();

        ClusterHealthRequestBuilder requestBuilder = clusterAdminClient.prepareHealth("_all");
        requestBuilder = requestBuilder.setTimeout(TimeValue.timeValueSeconds(120));

        ActionFuture<ClusterHealthResponse> healthFuture =
                clusterAdminClient.health(requestBuilder.request());

        ClusterHealthResponse health = healthFuture.actionGet(TimeValue.timeValueSeconds(120));

        assertThat(health.getNumberOfNodes(), greaterThanOrEqualTo(1));
        assertThat(health.getNumberOfDataNodes(), greaterThanOrEqualTo(1));
    }

    @Override
    protected void after() {

        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    Shutting down embedded Elasticsearch node ");
        LOG.info("-------------------------------------------------------------------------");

        try {
            embeddedNodeEnv.close();
            tempFolder.delete();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Client getClient() {
        return embeddedNodeEnv.getClient();
    }
}