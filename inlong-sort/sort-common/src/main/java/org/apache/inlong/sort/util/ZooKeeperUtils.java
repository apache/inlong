/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.util;

import com.google.common.base.Preconditions;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class containing helper functions to interact with ZooKeeper.
 *
 * <p>Copied from Flink project.</p>
 */
public class ZooKeeperUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtils.class);

    /**
     * Starts a {@link CuratorFramework} instance and connects it to the given ZooKeeper quorum.
     *
     * @param configuration {@link Configuration} object containing the configuration
     *         values
     * @return {@link CuratorFramework} instance
     */
    public static CuratorFramework startCuratorFramework(Configuration configuration) {
        Preconditions.checkNotNull(configuration, "configuration");
        final String zkQuorum = configuration.getValue(Constants.ZOOKEEPER_QUORUM);

        if (zkQuorum == null || StringUtils.isBlank(zkQuorum)) {
            throw new RuntimeException("No valid ZooKeeper quorum has been specified. "
                    + "You can specify the quorum via the configuration key '"
                    + Constants.ZOOKEEPER_QUORUM.key() + "'.");
        }

        final int sessionTimeout = configuration
                .getInteger(Constants.ZOOKEEPER_SESSION_TIMEOUT);

        final int connectionTimeout = configuration
                .getInteger(Constants.ZOOKEEPER_CONNECTION_TIMEOUT);

        final int retryWait = configuration.getInteger(Constants.ZOOKEEPER_RETRY_WAIT);

        final int maxRetryAttempts = configuration
                .getInteger(Constants.ZOOKEEPER_MAX_RETRY_ATTEMPTS);

        final boolean disableSaslClient = configuration
                .getBoolean(Constants.ZOOKEEPER_SASL_DISABLE);

        ACLProvider aclProvider;

        final ZkClientACLMode aclMode = ZkClientACLMode.fromConfig(configuration);

        if (disableSaslClient && aclMode == ZkClientACLMode.CREATOR) {
            String errorMessage =
                    "Cannot set ACL role to " + aclMode + "  since SASL authentication is "
                            + "disabled through the " + Constants.ZOOKEEPER_SASL_DISABLE.key()
                            + " property";
            LOG.warn(errorMessage);
            throw new IllegalStateException(errorMessage);
        }

        if (aclMode == ZkClientACLMode.CREATOR) {
            LOG.info("Enforcing creator for ZK connections");
            aclProvider = new SecureAclProvider();
        } else {
            LOG.info("Enforcing default ACL for ZK connections");
            aclProvider = new DefaultACLProvider();
        }

        final String root = configuration.getValue(Constants.ZOOKEEPER_ROOT);

        LOG.info("Using '{}' as Zookeeper namespace.", root);

        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(zkQuorum)
                .sessionTimeoutMs(sessionTimeout)
                .connectionTimeoutMs(connectionTimeout)
                .retryPolicy(new ExponentialBackoffRetry(retryWait, maxRetryAttempts))
                // Curator prepends a '/' manually and throws an Exception if the
                // namespace starts with a '/'.
                .namespace(root.startsWith("/") ? root.substring(1)
                        : root)
                .aclProvider(aclProvider)
                .build();

        cf.start();

        return cf;
    }

    /**
     * Returns the configured ZooKeeper quorum (and removes whitespace, because ZooKeeper does not
     * tolerate it).
     */
    public static String getZooKeeperEnsemble(Configuration config)
            throws IllegalStateException {

        String zkQuorum = config.getValue(Constants.ZOOKEEPER_QUORUM);

        if (zkQuorum == null || StringUtils.isBlank(zkQuorum)) {
            throw new IllegalStateException("No ZooKeeper quorum specified in config.");
        }

        // Remove all whitespace
        zkQuorum = zkQuorum.replaceAll("\\s+", "");

        return zkQuorum;
    }

    public static String generateZookeeperPath(String root, String namespace) {
        if (!namespace.startsWith("/")) {
            namespace = '/' + namespace;
        }

        if (namespace.endsWith("/")) {
            namespace = namespace.substring(0, namespace.length() - 1);
        }

        if (root.endsWith("/")) {
            root = root.substring(0, root.length() - 1);
        }

        return root + namespace;
    }

    /**
     * Returns a facade of the client that uses the specified namespace, and ensures that all nodes
     * in the path exist.
     *
     * @param client ZK client
     * @param path the new namespace
     * @return ZK Client that uses the new namespace
     * @throws Exception ZK errors
     */
    public static CuratorFramework useNamespaceAndEnsurePath(final CuratorFramework client,
            final String path) throws Exception {
        Preconditions.checkNotNull(client, "client must not be null");
        Preconditions.checkNotNull(path, "path must not be null");

        // Ensure that the checkpoints path exists
        client.newNamespaceAwareEnsurePath(path)
                .ensure(client.getZookeeperClient());

        // All operations will have the path as root
        return client.usingNamespace(generateZookeeperPath(client.getNamespace(), path));
    }

    /**
     * Create zk path with data.
     * @param zkClient zk client
     * @param nodePath the path to be created
     * @param data the data in the path to be created
     * @param createMode {@link CreateMode}
     */
    public static String createRecursive(
            CuratorFramework zkClient,
            String nodePath,
            byte[] data,
            CreateMode createMode) throws Exception {

        if (!nodePath.startsWith("/")) {
            throw KeeperException.create(Code.NONODE, "path " + nodePath
                    + " is invalid");
        }

        if (nodePath.endsWith("/")) {
            nodePath = nodePath.substring(0, nodePath.length() - 1);
        }

        int lastsp = nodePath.lastIndexOf("/");

        if (lastsp == 0) {
            return zkClient.create().withMode(createMode).forPath(nodePath);
        }

        String parent = nodePath.substring(0, lastsp);
        if (zkClient.checkExists().forPath(parent) == null) {
            createRecursive(zkClient, parent, null, CreateMode.PERSISTENT);
        }

        return zkClient.create().withMode(createMode).forPath(nodePath, data);
    }


    /**
     * Secure {@link ACLProvider} implementation.
     */
    public static class SecureAclProvider implements ACLProvider {

        @Override
        public List<ACL> getDefaultAcl() {
            return ZooDefs.Ids.CREATOR_ALL_ACL;
        }

        @Override
        public List<ACL> getAclForPath(String path) {
            return ZooDefs.Ids.CREATOR_ALL_ACL;
        }
    }

    /**
     * ZooKeeper client ACL mode enum.
     */
    public enum ZkClientACLMode {
        CREATOR,
        OPEN;

        /**
         * Return the configured {@link ZkClientACLMode}.
         *
         * @param config The config to parse
         * @return Configured ACL mode or the default defined by
         * {@link Constants#ZOOKEEPER_CLIENT_ACL}
         *         if not configured.
         */
        public static ZkClientACLMode fromConfig(Configuration config) {
            String aclMode = config.getString(Constants.ZOOKEEPER_CLIENT_ACL);
            if (aclMode == null || aclMode.equalsIgnoreCase(ZkClientACLMode.OPEN.name())) {
                return ZkClientACLMode.OPEN;
            } else if (aclMode.equalsIgnoreCase(ZkClientACLMode.CREATOR.name())) {
                return ZkClientACLMode.CREATOR;
            } else {
                String message = "Unsupported ACL option: [" + aclMode + "] provided";
                LOG.error(message);
                throw new IllegalStateException(message);
            }
        }
    }

    /**
     * Private constructor to prevent instantiation.
     */
    private ZooKeeperUtils() {
        throw new RuntimeException();
    }
}
