package org.apache.inlong.sort.elasticsearch5;

import org.elasticsearch.client.Client;

import java.io.File;

/**
 * The {@link EmbeddedElasticsearchNodeEnvironment} is used in integration tests to manage
 * Elasticsearch embedded nodes.
 *
 * <p>NOTE: In order for {@link ElasticsearchSinkTestBase} to dynamically load version-specific
 * implementations for the tests, concrete implementations must be named {@code
 * EmbeddedElasticsearchNodeEnvironmentImpl}. It must also be located under the same package. The
 * intentional package-private accessibility of this interface enforces that.
 */
public interface EmbeddedElasticsearchNodeEnvironment {

    /**
     * Start an embedded Elasticsearch node instance. Calling this method multiple times
     * consecutively should not restart the embedded node.
     *
     * @param tmpDataFolder The temporary data folder for the embedded node to use.
     * @param clusterName The name of the cluster that the embedded node should be configured with.
     */
    void start(File tmpDataFolder, String clusterName) throws Exception;

    /** Close the embedded node, if previously started. */
    void close() throws Exception;

    /**
     * Returns a client to communicate with the embedded node.
     *
     * @return Client to communicate with the embedded node. Returns {@code null} if the embedded
     *     node wasn't started or is closed.
     */
    Client getClient();
}