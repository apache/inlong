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

package org.apache.inlong.manager.service.queue;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarMessageInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarNamespacePolicies;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarPersistencePolicies;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarRetentionPolicies;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTenantInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTopicMetadata;
import org.apache.inlong.manager.service.resource.queue.pulsar.PulsarUtils;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Test class for Pulsar utils.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class PulsarUtilsTest {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarUtilsTest.class);

    public static final Network NETWORK = Network.newNetwork();

    private static final String INTER_CONTAINER_PULSAR_ALIAS = "pulsar";

    private static final Gson GSON = new GsonBuilder().create(); // thread safe

    private static final String DEFAULT_SERVICE_URL = "http://127.0.0.1:8080";

    private static final String DEFAULT_TENANT = "public";

    private static final String DEFAULT_CREATE_TENANT = "test_tenant";

    private static final String DEFAULT_NAMESPACE = "default";

    private static final int DEFAULT_PARTITIONS_NUM = 3;

    private static final String PERSISTENT_TOPIC_HEAD = "persistent://";

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private ResponseEntity<byte[]> byteExchange;

    @Mock
    private PulsarClusterInfo pulsarClusterInfo;

    private static PulsarNamespacePolicies policies;

    private static final PulsarContainer PULSAT_CONTAINER = new PulsarContainer(
            DockerImageName.parse("apachepulsar/pulsar:2.8.2")
                    .asCompatibleSubstituteFor("apachepulsar/pulsar"))
                            .withNetwork(NETWORK)
                            .withAccessToHost(true)
                            .withNetworkAliases(INTER_CONTAINER_PULSAR_ALIAS)
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    private static final RestTemplate client = new RestTemplate();

    private static final PulsarClusterInfo pulsarCluster = new PulsarClusterInfo();

    @BeforeAll
    public static void beforeAll() {
        policies = new PulsarNamespacePolicies();
        policies.setMessageTtlInSeconds(10);
        PulsarPersistencePolicies persistencePolicies = new PulsarPersistencePolicies();
        persistencePolicies.setBookkeeperEnsemble(3);
        persistencePolicies.setBookkeeperAckQuorum(3);
        persistencePolicies.setBookkeeperWriteQuorum(3);
        persistencePolicies.setManagedLedgerMaxMarkDeleteRate(4.0);
        policies.setPersistence(persistencePolicies);
        PulsarRetentionPolicies retentionPolicies = new PulsarRetentionPolicies();
        retentionPolicies.setRetentionSizeInMB(2048l);
        retentionPolicies.setRetentionTimeInMinutes(500);
        policies.setRetentionPolicies(retentionPolicies);

        PULSAT_CONTAINER.setPortBindings(Arrays.asList("6650:6650", "8080:8080"));
        Startables.deepStart(Stream.of(PULSAT_CONTAINER)).join();
        LOG.info("Containers are started.");
        pulsarCluster.setAdminUrl("http://127.0.0.1:8080");
    }

    @AfterAll
    public static void teardown() {
        if (PULSAT_CONTAINER != null) {
            PULSAT_CONTAINER.stop();
        }
    }

    @BeforeEach
    public void before() {
        when(pulsarClusterInfo.getAdminUrl()).thenReturn(DEFAULT_SERVICE_URL);
        when(pulsarClusterInfo.getToken()).thenReturn("testtoken");

    }
    /**
     * Test cases for {@link PulsarUtils#getClusters(RestTemplate, PulsarClusterInfo)}.
     *
     * @throws Exception
     */
    @Test
    public void testGetClusters() throws Exception {
        final String result = "[\"standalone\"]";
        List<String> expected = GSON.fromJson(result, ArrayList.class);
        List<String> clusters = PulsarUtils.getClusters(client, pulsarCluster);
        assertEquals(expected.size(), clusters.size());
        assertEquals(expected, clusters);
    }

    /**
     * Test cases for {@link PulsarUtils#getBrokers(RestTemplate, PulsarClusterInfo)}.
     *
     * @throws Exception
     */
    @Test
    public void testGetBrokers() throws Exception {
        final String result = "[\"localhost:8080\"]";
        List<String> expected = GSON.fromJson(result, ArrayList.class);
        List<String> brokers = PulsarUtils.getBrokers(client, pulsarCluster);
        assertEquals(expected.size(), brokers.size());
        assertEquals(expected, brokers);
    }

    /**
     * Test cases for {@link PulsarUtils#getTenants(RestTemplate, PulsarClusterInfo)}.
     *
     * @throws Exception
     */
    @Test
    public void testGetTenants() throws Exception {
        List<String> tenants = PulsarUtils.getTenants(client, pulsarCluster);
        assertNotNull(tenants);
    }

    /**
     * Test cases for {@link PulsarUtils#getNamespaces(RestTemplate, PulsarClusterInfo, String)}.
     *
     * @throws Exception
     */
    @Test
    public void testGetNamespaces() throws Exception {
        List<String> namespaces = PulsarUtils.getNamespaces(client, pulsarCluster, DEFAULT_TENANT);
        assertNotNull(namespaces);
    }

    /**
     * Test cases for {@link PulsarUtils#createTenant}.
     *
     * @throws Exception
     */
    @Test
    public void testCreateTenant() throws Exception {
        PulsarTenantInfo pulsarTenantInfo = new PulsarTenantInfo();
        pulsarTenantInfo.setAdminRoles(Sets.newHashSet());
        pulsarTenantInfo.setAllowedClusters(Sets.newHashSet("standalone"));
        PulsarUtils.createTenant(client, pulsarCluster, DEFAULT_CREATE_TENANT, pulsarTenantInfo);
        Thread.sleep(500);
        List<String> tenants = PulsarUtils.getTenants(client, pulsarCluster);
        assertTrue(tenants.contains(DEFAULT_CREATE_TENANT));
    }

    /**
     * Test cases for {@link PulsarUtils#createNamespace(RestTemplate, PulsarClusterInfo, String, String, PulsarNamespacePolicies)}.
     *
     * @throws Exception
     */
    @Test
    public void testCreateNamespace() throws Exception {
        final String namespaceName = "testCreateNamespace";
        final String namespaceInfo = DEFAULT_TENANT + InlongConstants.SLASH + namespaceName;
        String param = GSON.toJson(policies);
        param = param.replaceAll("messageTtlInSeconds", "message_ttl_in_seconds")
                .replaceAll("retentionPolicies", "retention_policies");
        final HttpHeaders headers = new HttpHeaders();
        if (StringUtils.isNotEmpty(pulsarClusterInfo.getToken())) {
            headers.add("Authorization", "Bearer " + pulsarClusterInfo.getToken());
        }
        final MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());

        PulsarUtils.createNamespace(client, pulsarCluster, DEFAULT_TENANT, namespaceName, policies);
        Thread.sleep(500);
        List<String> namespaces = PulsarUtils.getNamespaces(client, pulsarCluster, DEFAULT_TENANT);
        assertTrue(namespaces.contains(namespaceInfo));
    }

    /**
     * Test cases for {@link PulsarUtils#getTopics(RestTemplate, PulsarClusterInfo, String, String)}.
     *
     * @throws Exception
     */
    @Test
    public void testGetTopics() throws Exception {
        List<String> topics = PulsarUtils.getTopics(client, pulsarCluster, DEFAULT_TENANT,
                DEFAULT_NAMESPACE);
        assertTrue(topics.size() >= 0);
    }

    /**
     * Test cases for {@link PulsarUtils#getPartitionedTopics(RestTemplate, PulsarClusterInfo, String, String)}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_getList</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/partitioned
     * method is: GET
     * request body: none
     * response : ["string"]
     *
     * @throws Exception
     */
    @Test
    public void testGetPartitionedTopics() throws Exception {
        List<String> topics = PulsarUtils.getPartitionedTopics(client, pulsarCluster, DEFAULT_TENANT,
                DEFAULT_NAMESPACE);
        assertTrue(topics.size() >= 0);
    }

    /**
     * Test cases for {@link PulsarUtils#createNonPartitionedTopic(RestTemplate, PulsarClusterInfo, String)}.
     *
     * @throws Exception
     */
    @Test
    public void testCreateNonPartitionedTopic() throws Exception {
        final String topicPath = DEFAULT_TENANT + InlongConstants.SLASH + DEFAULT_NAMESPACE + InlongConstants.SLASH
                + "testCreateNonPartitionedTopic";
        final String topicInfo = PERSISTENT_TOPIC_HEAD + topicPath;

        Thread.sleep(500);
        PulsarUtils.createNonPartitionedTopic(client, pulsarCluster, topicPath);
        Thread.sleep(500);
        List<String> topics = PulsarUtils.getTopics(client, pulsarCluster, DEFAULT_TENANT,
                DEFAULT_NAMESPACE);
        assertTrue(topics.contains(topicInfo));
    }

    /**
     * Test cases for {@link PulsarUtils#createPartitionedTopic(RestTemplate, PulsarClusterInfo, String, Integer)}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_createPartitionedTopic</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions
     * method is: PUT
     * request body: integer <int32> (The number of partitions for the topic)
     *
     * @throws Exception
     */
    @Test
    public void testCreatePartitionedTopic() throws Exception {
        final String topicPath = DEFAULT_TENANT + InlongConstants.SLASH + DEFAULT_NAMESPACE + InlongConstants.SLASH
                + "testCreatePartitionedTopic";
        final String topicInfo = PERSISTENT_TOPIC_HEAD + topicPath;

        PulsarUtils.createPartitionedTopic(client, pulsarCluster, topicPath, DEFAULT_PARTITIONS_NUM);
        Thread.sleep(500);
        List<String> topics = PulsarUtils.getPartitionedTopics(client, pulsarCluster, DEFAULT_TENANT,
                DEFAULT_NAMESPACE);
        assertTrue(topics.contains(topicInfo));
    }

    /**
     * Test cases for {@link PulsarUtils#getInternalStatsPartitionedTopics(RestTemplate, PulsarClusterInfo, String)}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_getPartitionedStatsInternal</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitioned-internalStats
     * method is: GET
     * request body: none
     * response : json
     *
     * @throws Exception
     */
    @Test
    public void testGetInternalStatsPartitionedTopics() throws Exception {
        final String topicPath = DEFAULT_TENANT + InlongConstants.SLASH + DEFAULT_NAMESPACE + InlongConstants.SLASH
                + "testGetInternalStatsPartitionedTopics";

        PulsarUtils.createPartitionedTopic(client, pulsarCluster, topicPath, DEFAULT_PARTITIONS_NUM);
        Thread.sleep(500);
        JsonObject stats = PulsarUtils.getInternalStatsPartitionedTopics(client, pulsarCluster, topicPath);
        assertNotNull(stats);
    }

    /**
     * Test cases for {@link PulsarUtils#getPartitionedTopicMetadata(RestTemplate, PulsarClusterInfo, String)}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_getPartitionedMetadata</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions
     * method is: GET
     * request body: none
     * response : json
     *
     * @throws Exception
     */
    @Test
    public void testGetPartitionedTopicMetadata() throws Exception {
        final String topicPath = DEFAULT_TENANT + InlongConstants.SLASH + DEFAULT_NAMESPACE + InlongConstants.SLASH
                + "testGetPartitionedTopicMetadata";
        PulsarUtils.createPartitionedTopic(client, pulsarCluster, topicPath, DEFAULT_PARTITIONS_NUM);
        Thread.sleep(500);
        PulsarTopicMetadata metadata = PulsarUtils.getPartitionedTopicMetadata(client, pulsarCluster,
                topicPath);
        assertNotNull(metadata);
    }

    /**
     * Test cases for {@link PulsarUtils#deleteNonPartitionedTopic(RestTemplate, PulsarClusterInfo, String)}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_deleteTopic</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}
     * method is: DELETE
     * request body: none
     *
     * @throws Exception
     */
    @Test
    public void testDeleteNonPartitionedTopic() throws Exception {
        final String topicPath = DEFAULT_TENANT + InlongConstants.SLASH + DEFAULT_NAMESPACE + InlongConstants.SLASH
                + "testDeleteNonPartitionedTopic";
        final String topicInfo = PERSISTENT_TOPIC_HEAD + topicPath;

        PulsarUtils.createNonPartitionedTopic(client, pulsarCluster, topicPath);
        Thread.sleep(500);
        List<String> topics = PulsarUtils.getTopics(client, pulsarCluster, DEFAULT_TENANT, DEFAULT_NAMESPACE);
        assertTrue(topics.contains(topicInfo));

        PulsarUtils.deleteNonPartitionedTopic(client, pulsarCluster, topicPath);
        Thread.sleep(500);
        topics = PulsarUtils.getTopics(client, pulsarCluster, DEFAULT_TENANT, DEFAULT_NAMESPACE);
        assertTrue(!topics.contains(topicInfo));
    }

    /**
     * Test cases for {@link PulsarUtils#forceDeleteNonPartitionedTopic(RestTemplate, PulsarClusterInfo, String)}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_deleteTopic</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}
     * method is: DELETE
     * request body: none
     *
     * @throws Exception
     */
    @Test
    public void testForceDeleteNonPartitionedTopic() throws Exception {
        final String topicPath = DEFAULT_TENANT + InlongConstants.SLASH + DEFAULT_NAMESPACE + InlongConstants.SLASH
                + "testForceDeleteNonPartitionedTopic";
        final String topicInfo = PERSISTENT_TOPIC_HEAD + topicPath;

        PulsarUtils.createNonPartitionedTopic(client, pulsarCluster, topicPath);
        Thread.sleep(500);
        List<String> topics = PulsarUtils.getTopics(client, pulsarCluster, DEFAULT_TENANT, DEFAULT_NAMESPACE);
        assertTrue(topics.contains(topicInfo));

        PulsarUtils.forceDeleteNonPartitionedTopic(client, pulsarCluster, topicPath);
        Thread.sleep(500);
        topics = PulsarUtils.getTopics(client, pulsarCluster, DEFAULT_TENANT, DEFAULT_NAMESPACE);
        assertTrue(!topics.contains(topicInfo));
    }

    /**
     * Test cases for {@link PulsarUtils#deletePartitionedTopic(RestTemplate, PulsarClusterInfo, String)}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_deletePartitionedTopic</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions
     * method is: DELETE
     * request body: none
     *
     * @throws Exception
     */
    @Test
    public void testDeletePartitionedTopic() throws Exception {
        final String topicPath = DEFAULT_TENANT + InlongConstants.SLASH + DEFAULT_NAMESPACE + InlongConstants.SLASH
                + "testDeletePartitionedTopic";
        final int numPartitions = 3;
        final String topicInfo = PERSISTENT_TOPIC_HEAD + topicPath;

        PulsarUtils.createPartitionedTopic(client, pulsarCluster, topicPath, numPartitions);
        Thread.sleep(500);
        List<String> topics = PulsarUtils.getPartitionedTopics(client, pulsarCluster, DEFAULT_TENANT,
                DEFAULT_NAMESPACE);
        assertTrue(topics.contains(topicInfo));

        PulsarUtils.deletePartitionedTopic(client, pulsarCluster, topicPath);
        Thread.sleep(500);
        topics = PulsarUtils.getPartitionedTopics(client, pulsarCluster, DEFAULT_TENANT, DEFAULT_NAMESPACE);
        assertTrue(!topics.contains(topicInfo));
    }

    /**
     * Test cases for {@link PulsarUtils#forceDeletePartitionedTopic(RestTemplate, PulsarClusterInfo, String)}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_deletePartitionedTopic</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions
     * method is: DELETE
     * request body: none
     *
     * @throws Exception
     */
    @Test
    public void testForceDeletePartitionedTopic() throws Exception {
        final String topicPath = DEFAULT_TENANT + InlongConstants.SLASH + DEFAULT_NAMESPACE + InlongConstants.SLASH
                + "testForceDeletePartitionedTopic";
        final int numPartitions = 3;
        final String topicInfo = PERSISTENT_TOPIC_HEAD + topicPath;

        PulsarUtils.createPartitionedTopic(client, pulsarCluster, topicPath, numPartitions);
        Thread.sleep(500);
        List<String> topics = PulsarUtils.getPartitionedTopics(client, pulsarCluster, DEFAULT_TENANT,
                DEFAULT_NAMESPACE);
        assertTrue(topics.contains(topicInfo));

        PulsarUtils.deletePartitionedTopic(client, pulsarCluster, topicPath);
        Thread.sleep(500);
        topics = PulsarUtils.getPartitionedTopics(client, pulsarCluster, DEFAULT_TENANT, DEFAULT_NAMESPACE);
        assertTrue(!topics.contains(topicInfo));
    }

    /**
     * Test cases for {@link PulsarUtils#lookupTopic(RestTemplate, PulsarClusterInfo, String)}.
     *
     * @throws Exception
     */
    @Test
    public void testLookupTopic() throws Exception {
        final String topicPath =
                DEFAULT_TENANT + InlongConstants.SLASH + DEFAULT_NAMESPACE + InlongConstants.SLASH + "testLookupTopic";
        PulsarUtils.createNonPartitionedTopic(client, pulsarCluster, topicPath);
        Thread.sleep(500);
        String actual = PulsarUtils.lookupTopic(client, pulsarCluster, topicPath);
        assertNotNull(actual);
    }

    /**
     * Test cases for {@link PulsarUtils#lookupPartitionedTopic(RestTemplate, PulsarClusterInfo, String)}.
     *
     * @throws Exception
     */
    @Test
    public void testLookupPartitionedTopic() throws Exception {
        final String topicPath = DEFAULT_TENANT + InlongConstants.SLASH + DEFAULT_NAMESPACE + InlongConstants.SLASH
                + "testLookupPartitionedTopic";
        PulsarUtils.createNonPartitionedTopic(client, pulsarCluster, topicPath);
        Thread.sleep(500);
        Map<String, String> actual = PulsarUtils.lookupPartitionedTopic(client, pulsarCluster, topicPath);
        assertNotNull(actual);
    }

    /**
     * Test cases for {@link PulsarUtils#getSubscriptions(RestTemplate, PulsarClusterInfo, String)}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_getSubscriptions</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscriptions
     * method is: GET
     * response body: ["string"]
     *
     * @throws Exception
     */
    @Test
    public void testGetSubscriptions() throws Exception {
        final String topicPath = DEFAULT_TENANT + InlongConstants.SLASH + DEFAULT_NAMESPACE + InlongConstants.SLASH
                + "testGetSubscriptions";
        PulsarUtils.createNonPartitionedTopic(client, pulsarCluster, topicPath);
        Thread.sleep(500);
        List<String> actual = PulsarUtils.getSubscriptions(client, pulsarCluster, topicPath);
        assertTrue(actual.size() >= 0);
    }

    /**
     * Test cases for {@link PulsarUtils#createSubscription(RestTemplate, PulsarClusterInfo, String, String)}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_createSubscription</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{subscriptionName}
     * method is: PUT
     * response body: json
     *
     * @throws Exception
     */
    @Test
    public void testCreateSubscription() throws Exception {
        final String subscriptionName = "test_subscription";
        final String topicPath = DEFAULT_TENANT + InlongConstants.SLASH + DEFAULT_NAMESPACE + InlongConstants.SLASH
                + "testCreateSubscription";
        PulsarUtils.createSubscription(client, pulsarCluster, topicPath, subscriptionName);
        Thread.sleep(500);
        List<String> actual = PulsarUtils.getSubscriptions(client, pulsarCluster, topicPath);
        assertTrue(actual.contains(subscriptionName));
    }

    /**
     * Test cases for {@link PulsarUtils#examineMessage(RestTemplate, PulsarClusterInfo, String, String, int)}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_examineMessage</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/examinemessage
     * method is: GET
     * request parameters:
     *   - initialPosition: Relative start position to examine message.It can be 'latest' or 'earliest'
     *   - messagePosition: The position of messages (default 1)
     *   - authoritative: Whether leader broker redirected this call to this broker. For internal use.
     * response body: byteArray
     *
     * @throws Exception
     */
    @Test
    public void testExamineMessage() throws Exception {
        /*
         * Since admin API cannot send messages to the topic, this test case will be simulated using mockito.
         */
        final String messageType = "latest";
        final int messagePosition = 1;
        String topicPath = DEFAULT_TENANT + InlongConstants.SLASH + DEFAULT_NAMESPACE + InlongConstants.SLASH
                + "testtopic-partition-1";
        StringBuilder urlBuilder = new StringBuilder().append(DEFAULT_SERVICE_URL)
                .append(PulsarUtils.QUERY_PERSISTENT_PATH).append("/").append(topicPath).append("/examinemessage")
                .append("?initialPosition=").append(messageType).append("&messagePosition=").append(messagePosition);
        final String expected = "test message!";

        when(restTemplate.exchange(eq(urlBuilder.toString()), eq(HttpMethod.GET), any(HttpEntity.class),
                eq(byte[].class))).thenReturn(byteExchange);
        when(byteExchange.getStatusCode()).thenReturn(HttpStatus.OK);
        when(byteExchange.getBody()).thenReturn(expected.getBytes(StandardCharsets.UTF_8));

        ResponseEntity<byte[]> response = PulsarUtils.examineMessage(restTemplate, pulsarClusterInfo, topicPath,
                messageType, messagePosition);
        assertEquals(expected, new String(response.getBody()));
    }

    /**
     * The case only supports local testing.
     *
     * @throws Exception
     */
    @Ignore
    public void localTest() throws Exception {
        RestTemplate restTemplate = new RestTemplate();
        PulsarClusterInfo pulsarClusterInfo = new PulsarClusterInfo();
        pulsarClusterInfo.setAdminUrl("http://127.0.0.1:8080");
        String topic = "public/test_pulsar_group/test_pulsar_stream";

        ResponseEntity<byte[]> pulsarMessage =
                PulsarUtils.examineMessage(restTemplate, pulsarClusterInfo,
                        topic, "latest", 1);
        List<PulsarMessageInfo> result = PulsarUtils.getMessagesFromHttpResponse(pulsarMessage, topic);
        System.out.println(result);
    }
}
